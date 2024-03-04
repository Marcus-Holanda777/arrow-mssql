import pyarrow as pa
from .connector import raw_sql
from textwrap import dedent
from typing import (
    Iterable, 
)
from .schemas import get_schema


def cursor_arrow(
    driver: str,
    name: str,
    database: str,
    query: bool = False,
    limit: int = 1_000_000
) -> Iterable[list]:
    
    with raw_sql(driver) as cursor:
        stmt = dedent(
            f'''
            SELECT 
            *
            FROM {database}.dbo.{name} 
            WITH(NOLOCK)
            '''
        )

        if query:
            stmt = dedent(name)

        cursor.execute(stmt)
        while batch := cursor.fetchmany(limit):
            yield batch


def to_arrow_lotes(
    driver: str,
    name: str,
    database: str,
    query: bool = False,
    limit: int = 1_000_000
) -> pa.ipc.RecordBatchReader:
    
    schema = get_schema(driver, name, database, query)
    array_type = schema('st')

    arrays = (
        pa.array(map(tuple, lote), type=array_type)
        for lote in cursor_arrow(
            driver, name, 
            database, query, limit
        )
    )

    lotes = map(
        pa.RecordBatch.from_struct_array, 
        arrays
    )

    return pa.ipc.RecordBatchReader.from_batches(
        schema('sh'), 
        lotes
    )


def to_parquet(
    driver: str,
    name: str,
    *,
    path: str,
    database: str,
    query: bool = False,
    limit: int = 1_000_000
) -> None:
    """
    ### Exporta tabela ou consulta para .parquet
    
    ----
    driver: String de conexao `pyodbc`
    name: Tabela ou consulta
    path: Caminho do arquivo
    database: Banco de dados da tabela
    query: Falso para tabela
    limit: porcao de dados
    """
    
    import pyarrow.parquet as pq
    
    with to_arrow_lotes(driver, name, database, query, limit) as lotes:
        with pq.ParquetWriter(path, lotes.schema) as writer:
            for lote in lotes:
                writer.write_batch(lote, row_group_size=limit)