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
    limit: int = 10_000
) -> Generator[Any, Any, None]:
    

    with contextlib.suppress(AttributeError):
        if isinstance(path, str):
            path = Path(path)

        tbl = pq.ParquetFile(path)

        droptable = db.drop_table(name)
        create = db.create_table(name, tbl.schema_arrow)
        insert = db.insert_table(name, tbl.schema_arrow)
        insert_puts = db.insert_setinputsizes(tbl.schema_arrow)

    
    with raw_sql(driver, autocommit=False) as cursor:

        if override:
            cursor.execute(droptable)
            cursor.execute(create)
            cursor.commit()

        cursor.fast_executemany = True
        cursor.setinputsizes(insert_puts)

        for rows in tbl.iter_batches(limit):
            lotes = [tuple(d.values()) for d in rows.to_pylist()]
            cursor.executemany(insert, lotes)

        cursor.commit()
        
        yield cursor