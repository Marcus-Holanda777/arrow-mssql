import pyarrow as pa


def decimal_mssql(tp: pa.DataType) -> str:
    
    precision = tp.precision
    scale = tp.scale

    if precision <= 28 and scale is None:
        return f'numeric({precision})'
    elif precision <=28 and scale >= 0:
        return f'decimal({precision}, {scale})'
    elif precision > 38:
        return f'decimal({precision}, {scale})'


def timestamp_mssql(tp: pa.DataType) -> str:
    unit = tp.unit

    if unit == 's':
        return 'smalldatetime'
    elif unit == 'ms':
        return 'datetime'
    elif unit == 'us':
        return 'datetimeoffset'
    elif unit == 'ns':
        return 'datetime2'


def times_mssql(tp: pa.DataType) -> str:
    unit = tp.unit

    if unit == 's':
        return 'time'
    elif unit == 'ms':
        return 'time(3)'
    elif unit == 'us':
        return 'time(6)'
    elif unit == 'ns':
        return 'time(9)'


def get_type_arrow(tp: pa.DataType) -> str | None:
    if pa.types.is_decimal(tp):
        return decimal_mssql(tp)
    
    if pa.types.is_timestamp(tp):
        return timestamp_mssql(tp)
    
    if pa.types.is_time(tp):
        return times_mssql(tp)
    
    return None


map_typs = {
    pa.int8(): 'tinyint',
    pa.int16(): 'smallint',
    pa.int32(): 'int',
    pa.int64(): 'bigint',
    pa.bool_(): 'bit',
    pa.float16(): 'real',
    pa.float32(): 'real',
    pa.float64(): 'double',
    pa.date32(): 'date',
    pa.date64(): 'date',
    pa.string(): 'varchar(max)',
    pa.utf8(): 'varchar(max)',
    pa.binary(): 'binary',
}