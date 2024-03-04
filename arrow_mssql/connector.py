import pyodbc as odbc
from typing import Any
from contextlib import closing
import contextlib


def do_connect(
    *args,
    **kwargs: Any,
) -> None:
    
    con = odbc.connect(
        *args,
        **kwargs,
    )

    with closing(con.cursor()) as cur:
        cur.execute("SET DATEFIRST 1")

    return con


@contextlib.contextmanager
def raw_sql(*args,**kwargs):

    con = do_connect(*args, **kwargs)
    cursor = con.cursor()

    try:
        yield cursor
    except Exception:
        raise
    finally:
        cursor.close()