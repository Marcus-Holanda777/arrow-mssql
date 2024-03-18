import pyarrow as pa
from .connector import raw_sql
from textwrap import dedent
from typing import (
    Iterable, 
)
from .schemas import get_schema
from .utils import is_query
import sqlglot as sg


def cursor_arrow(
    driver: str,
    name: str,
    database: str,
    schema: str = 'dbo',
    limit: int = 1_000_000
) -> Iterable[list]:
    
    with raw_sql(driver) as cursor:
        stmt = (
            sg.select('*')
            .from_(
                sg.table(
                    f'{name} WITH(NOLOCK)',
                    db=schema,
                    catalog=database,
                    quoted=''
                )
            )
            .sql('tsql')
        )

        if is_query(name):
            stmt = dedent(name)

        cursor.execute(stmt)
        while batch := cursor.fetchmany(limit):
            yield batch


def to_arrow_lotes(
    driver: str,
    name: str,
    database: str,
    schema: str = 'dbo',
    limit: int = 1_000_000
) -> pa.ipc.RecordBatchReader:
    
    schema_arrow = get_schema(
        driver, 
        name, 
        database, 
        schema
    )
    array_type = schema_arrow('st')
    
    arrays = (
        pa.array(map(tuple, lote), type=array_type)
        for lote in cursor_arrow(
            driver, 
            name, 
            database, 
            schema,
            limit   
        )
    )

    lotes = map(
        pa.RecordBatch.from_struct_array, 
        arrays
    )

    return pa.ipc.RecordBatchReader.from_batches(
        schema_arrow('sh'), 
        lotes
    )


def to_parquet(
    driver: str,
    name: str,
    *,
    path: str,
    database: str,
    schema: str = 'dbo',
    row_group_size: int = 1_000_000,
    limit: int = 1_000_000
) -> None:
    """
    ### Exporta tabela ou consulta para .parquet
    
    ----
    driver: String de conexao `pyodbc`
    name: Tabela ou consulta
    path: Caminho do arquivo
    database: Banco de dados da tabela
    schema: schema da tabela
    row_group_size: Grupo do arquivo .parquet
    limit: porcao de dados
    """
    
    import pyarrow.parquet as pq
    
    with to_arrow_lotes(
        driver, 
        name, 
        database, 
        schema, 
        limit
    ) as lotes:
        
        with pq.ParquetWriter(path, lotes.schema) as writer:
            for lote in lotes:
                writer.write_batch(
                    lote, 
                    row_group_size=row_group_size
                )


def to_csv(
    driver: str,
    name: str,
    *,
    path: str,
    database: str,
    schema: str = 'dbo',
    delimiter: str = ';',
    limit: int = 1_000_000
) -> None:
    """
    ### Exporta tabela ou consulta para .csv
    
    ----
    driver: String de conexao `pyodbc`
    name: Tabela ou consulta
    path: Caminho do arquivo
    database: Banco de dados da tabela
    schema: schema da tabela
    delimiter: separador das colunas
    limit: porcao de dados
    """
    
    from pyarrow import csv
    
    with to_arrow_lotes(
        driver, 
        name, 
        database, 
        schema, 
        limit
    ) as lotes:
        
        write_options = csv.WriteOptions(
            include_header=True,
            delimiter=delimiter, 
            quoting_style='all_valid'
        )
        
        with csv.CSVWriter(
            path, 
            lotes.schema, 
            write_options=write_options
        ) as writer:
            for lote in lotes:
                writer.write_batch(lote)