import pyarrow.parquet as pq
from arrow_mssql.connector import raw_sql
import contextlib
from pathlib import Path
import arrow_mssql.input.schema as db
from typing import (
    Generator,
    Any
)


@contextlib.contextmanager
def write_parquet(
    driver: str,
    name: str,
    *,
    path: str | Path,
    override: bool = True,
    limit: int = None,
    chunk_size: int = 100_000
) -> Generator[Any, Any, None]:
    
    with contextlib.suppress(AttributeError):
        if isinstance(path, str):
            path = Path(path)

        tbl = pq.ParquetFile(path)

        droptable = db.drop_table(name)
        create = db.create_table(name, tbl.schema_arrow)
        insert = db.insert_table(name, tbl.schema_arrow)
        insert_puts = db.insert_setinputsizes(tbl.schema_arrow)

    # NOTE: autocommit pyodbc default FALSE
    with raw_sql(driver) as cursor:

        if override:
            cursor.execute(droptable)
            cursor.execute(create)

        cursor.fast_executemany = True
        cursor.setinputsizes(insert_puts)
        
        inserts = 0
        if limit:
            chunk_size = (
                limit 
                if limit < chunk_size
                else chunk_size
            )

        for rows in tbl.iter_batches(chunk_size):

            lotes = [tuple(d.values()) for d in rows.to_pylist()]
            
            if limit:
                inserts += len(lotes)
                if inserts > limit:
                    fatia = (limit - (inserts - len(lotes)))
                    lotes = lotes[:fatia]

            cursor.executemany(insert, lotes)
            
            if limit:
                if (
                    inserts % limit == 0 
                    or inserts > limit
                ):
                    break

        yield cursor