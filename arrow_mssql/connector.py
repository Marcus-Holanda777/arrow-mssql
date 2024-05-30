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
    **kwargs: Any
) -> None:
    
    con = odbc.connect(
        *args,
        **kwargs,
    )

    con.add_output_converter(
        -155, 
        datetimeoffset_to_datetime
    )
    
    con.autocommit = True
    con.set_attr(
        odbc.SQL_ATTR_TXN_ISOLATION,
        odbc.SQL_TXN_READ_UNCOMMITTED
    )
    con.autocommit = False # habilita transacoes

    with closing(con.cursor()) as cur:
        cur.execute("SET DATEFIRST 1")

    return con


@contextlib.contextmanager
def raw_sql(*args,**kwargs):

    con = do_connect(*args, **kwargs)
    cursor = con.cursor()

    # NOTE: Adicionado o commit na conexao !
    try:
        yield cursor
    except Exception:  
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        cursor.close()
        con.close()