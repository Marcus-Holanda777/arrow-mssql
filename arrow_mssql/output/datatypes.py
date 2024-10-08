import pyarrow as pa
from uuid import UUID


class UuidScalarType(pa.ExtensionScalar):
    def as_py(self):
        return None if self.value is None else UUID(bytes=self.value.as_py())


class UuidType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(16), 'pacote.UuidType')

    def __arrow_ext_scalar_class__(self):
        return UuidScalarType

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def decimal_arrow(
    precision, 
    scale
) -> pa.DataType:
    
    if precision > 38:
        dtype = pa.decimal256(precision, scale)
    else:
        dtype = pa.decimal128(precision, scale)
    return dtype


def float_arrow(
    precision
) -> pa.DataType:
    
    if precision <= 24:
        return pa.float32()
    else:
        return pa.float64()


def time_arrow(
    precision
):
    if precision is None or precision == 0:
        return pa.time32('s')
    elif 1 <= precision <= 3:
        return pa.time32('ms')
    elif 4 <= precision <= 6:
        return pa.time64('us')
    elif 7 <= precision <= 9:
        return pa.time64('ns')


def timestamp_arrow(
    precision
) -> pa.DataType:
    
    if precision is None or precision == 0:
        return pa.timestamp('s')
    elif 1 <= precision <= 3:
        return pa.timestamp('ms')
    elif 4 <= precision <= 6:
        return pa.timestamp('us')
    elif 7 <= precision <= 9:
        return pa.timestamp('ns')


# NOTE: Ainda em testes @EXPERIMENTAL
UUID_TYPE_ARROW = UuidType()
pa.register_extension_type(UUID_TYPE_ARROW)

map_typs = {
    'int': pa.int32(),
    'bit': pa.bool_(),
    'char': pa.string(),
    'nchar': pa.string(),
    'text': pa.string(),
    'ntext': pa.string(),
    'binary': pa.binary(),
    'image': pa.binary(),
    'varbinary': pa.binary(),
    'timestamp': pa.binary(),
    'datetime': timestamp_arrow,
    'datetime2': timestamp_arrow,
    'smalldatetime': timestamp_arrow,
    'datetimeoffset': timestamp_arrow,
    'numeric': decimal_arrow,
    'decimal': decimal_arrow,
    'smallint': pa.int16(),
    'varchar': pa.string(),
    'nvarchar': pa.string(),
    'date': pa.date64(),
    'float': float_arrow,
    'real': pa.float32(),
    'bigint': pa.int64(),
    'money': pa.int64(),
    'smallmoney': pa.int32(),
    'tinyint': pa.int8(),
    'time': time_arrow,
    'uniqueidentifier': pa.string(),
    'xml': pa.string()
}