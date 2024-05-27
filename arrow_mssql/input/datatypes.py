import pyarrow as pa
import pyodbc


def decimal_mssql(tp: pa.DataType) -> str:
    
    precision = tp.precision
    scale = tp.scale

    if precision <= 28 and scale is None:
        return f'numeric({precision})'
    elif precision <=28 and scale >= 0:
        return f'decimal({precision}, {scale})'
    elif precision > 38:
        return f'decimal({precision}, {scale})'
    

def decimal_mssql_pyodbc(tp: pa.DataType) -> tuple:
    
    precision = tp.precision
    scale = tp.scale

    if precision <= 28 and scale is None:
        return (pyodbc.SQL_NUMERIC, precision, 0)
    elif precision <=28 and scale >= 0:
        return (pyodbc.SQL_DECIMAL, precision, scale)
    elif precision > 38:
        return (pyodbc.SQL_DECIMAL, precision, scale)


def timestamp_mssql(tp: pa.DataType) -> str:
    unit = tp.unit

    if unit == 's':
        return 'smalldatetime'
    elif unit == 'ms':
        return 'datetime'
    elif unit == 'us':
        # 'datetimeoffset'
        return 'datetime2'
    elif unit == 'ns':
        return 'datetime2'
    

def timestamp_mssql_pyodbc(tp: pa.DataType) -> tuple:
    unit = tp.unit

    if unit == 's':
        return (pyodbc.SQL_TYPE_TIMESTAMP, 0, 0)
    elif unit == 'ms':
        return (pyodbc.SQL_TYPE_TIMESTAMP, 3, 0)
    elif unit == 'us':
        return (pyodbc.SQL_TYPE_TIMESTAMP, 6, 0)
    elif unit == 'ns':
        return (pyodbc.SQL_TYPE_TIMESTAMP, 7, 0)


def times_mssql(tp: pa.DataType) -> str:
    unit = tp.unit

    if unit == 's':
        return 'time'
    elif unit == 'ms':
        return 'time(3)'
    elif unit == 'us':
        return 'time(6)'
    elif unit == 'ns':
        return 'time(7)'
    

def times_mssql_pyodbc(tp: pa.DataType) -> str:
    unit = tp.unit

    if unit == 's':
        return (pyodbc.SQL_TYPE_TIME, 0, 0)
    elif unit == 'ms':
        return (pyodbc.SQL_TYPE_TIME, 3, 0)
    elif unit == 'us':
        return (pyodbc.SQL_TYPE_TIME, 6, 0)
    elif unit == 'ns':
        return (pyodbc.SQL_TYPE_TIME, 7, 0)


def get_type_arrow(tp: pa.DataType) -> str | None:
    if pa.types.is_decimal(tp):
        return decimal_mssql(tp)
    
    if pa.types.is_timestamp(tp):
        return timestamp_mssql(tp)
    
    if pa.types.is_time(tp):
        return times_mssql(tp)
    
    return None


def get_type_arrow_pyodbc(tp: pa.DataType) -> tuple | None:
    if pa.types.is_decimal(tp):
        return decimal_mssql_pyodbc(tp)
    
    if pa.types.is_timestamp(tp):
        return timestamp_mssql_pyodbc(tp)
    
    if pa.types.is_time(tp):
        return times_mssql_pyodbc(tp)
    
    return None


map_typs = {
    pa.int8(): 'tinyint',
    pa.int16(): 'smallint',
    pa.int32(): 'int',
    pa.int64(): 'bigint',
    pa.bool_(): 'bit',
    pa.float16(): 'real',
    pa.float32(): 'real',
    pa.float64(): 'float',
    pa.date32(): 'date',
    pa.date64(): 'date',
    pa.string(): 'varchar(max)',
    pa.utf8(): 'varchar(max)',
    pa.binary(): 'binary',
}

map_typs_pyodbc = {
    pa.int8(): pyodbc.SQL_TINYINT,
    pa.int16(): pyodbc.SQL_SMALLINT,
    pa.int32(): pyodbc.SQL_INTEGER,
    pa.int64(): pyodbc.SQL_BIGINT,
    pa.bool_(): pyodbc.SQL_BIGINT,
    pa.float16(): pyodbc.SQL_REAL,
    pa.float32(): pyodbc.SQL_REAL,
    pa.float64(): pyodbc.SQL_FLOAT,
    pa.date32(): pyodbc.SQL_TYPE_DATE,
    pa.date64(): pyodbc.SQL_TYPE_DATE,
    pa.string(): (pyodbc.SQL_VARCHAR, 0, 0),
    pa.utf8(): (pyodbc.SQL_VARCHAR, 0, 0),
    pa.binary(): pyodbc.SQL_BINARY,
}