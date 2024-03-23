import pyodbc as odbc
from typing import Any
from contextlib import closing
import contextlib
import datetime
import struct


def datetimeoffset_to_datetime(value):
    (
        year, 
        month, 
        day, 
        hour, 
        minute, 
        second, 
        frac, 
        tz_hour, 
        tz_minutes
    ) = struct.unpack("<6hI2h", value)

    return datetime.datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        frac // 1000,
        datetime.timezone(datetime.timedelta(hours=tz_hour, minutes=tz_minutes)),
    )


def do_connect(
    *args,
    **kwargs: Any,
) -> None:
    
    con = odbc.connect(
        *args,
        **kwargs,
    )

    con.add_output_converter(
        -155, 
        datetimeoffset_to_datetime
    )
    
    # with(nolock) ativado
    con.execute(
        'set transaction '
        'isolation level '
        'read uncommitted;'
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