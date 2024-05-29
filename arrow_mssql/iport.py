import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as csv
from arrow_mssql.connector import raw_sql
import contextlib
from pathlib import Path
import arrow_mssql.input.schema as db
from typing import (
    Generator,
    Any,
    Callable
)

def limite_rows(
    inserts: int = 0, 
    limit: int = None
) -> Callable:
    
    inserts = 0
    def inner(rows) -> Generator[tuple, Any, None]:
        nonlocal inserts
        for row in rows.to_pylist():
            if limit:
                if inserts < limit:
                    inserts += 1
                    yield tuple(row.values())
            else:      
                yield tuple(row.values())

    return inner


@contextlib.contextmanager
def write_parquet(
    driver: str,
    name: str,
    *,
    path: str | Path,
    override: bool = True,
    schema: str = 'dbo',
    columns: list | None = None,
    limit: int = None,
    chunk_size: int = 100_000
) -> Generator[Any, Any, None]:
    
    with contextlib.suppress(AttributeError):
        if isinstance(path, str):
            path = Path(path)

        tbl = pq.ParquetFile(path)
        tbl_schema = tbl.schema_arrow

        if columns:
            tbl_schema = pa.schema([
                col
                for col in tbl.schema_arrow
                if col.name in columns
            ])
        
        tbl_name = f'{schema}.{name}'
        droptable = db.drop_table(tbl_name)
        create = db.create_table(tbl_name, tbl_schema)
        insert = db.insert_table(tbl_name, tbl_schema)
        insert_puts = db.insert_setinputsizes(tbl_schema)

    # NOTE: autocommit pyodbc default FALSE
    with raw_sql(driver) as cursor:

        if override:
            cursor.execute(droptable)
            cursor.execute(create)

        cursor.fast_executemany = True
        cursor.setinputsizes(insert_puts)
        
        if limit:
            chunk_size = (
                limit 
                if limit < chunk_size
                else chunk_size
            )

        lotes_limit = limite_rows(limit=limit)

        for rows in tbl.iter_batches(chunk_size, columns=columns):
            lotes = list(lotes_limit(rows))
            if not lotes:
                break

            cursor.executemany(insert, lotes)

        yield cursor


@contextlib.contextmanager
def write_csv(
    driver: str,
    name: str,
    *,
    path: str | Path,
    override: bool = True,
    schema: str = 'dbo',
    columns: list | None = None,
    limit: int = None,
    delimiter: str = ';',
    block_size: int = 1 << 20
) -> Generator[Any, Any, None]:
    """
    MEGA BYTES = 1 << 20
    """
    
    read_options = csv.ReadOptions(
        block_size=block_size
    )

    parse_options = csv.ParseOptions(
        delimiter=delimiter
    )
    
    convert_options = None
    if columns:
        convert_options = csv.ConvertOptions(
            include_columns=columns
        )
    
    with contextlib.suppress(AttributeError):
        if isinstance(path, str):
            path = Path(path)

        tbl = csv.open_csv(
            path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        tbl_schema = tbl.schema
        
        tbl_name = f'{schema}.{name}'
        droptable = db.drop_table(tbl_name)
        create = db.create_table(tbl_name, tbl_schema)
        insert = db.insert_table(tbl_name, tbl_schema)
        insert_puts = db.insert_setinputsizes(tbl_schema)

    # NOTE: autocommit pyodbc default FALSE
    with raw_sql(driver) as cursor:

        if override:
            cursor.execute(droptable)
            cursor.execute(create)

        cursor.fast_executemany = True
        cursor.setinputsizes(insert_puts)
        
        lotes_limit = limite_rows(limit=limit)

        for rows in tbl:
            lotes = list(lotes_limit(rows))
            if not lotes:
                break

            cursor.executemany(insert, lotes)
       
        yield cursor