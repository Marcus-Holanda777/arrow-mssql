from arrow_mssql.connector import raw_sql
from arrow_mssql.input.datatypes import get_type_arrow, map_typs
import pyarrow as pa
import textwrap


DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)


def schema_mssql(tbl: str, schema_arrow: pa.Schema):
    list_types = {*{}}

    for field in schema_arrow:
        col = field.name
        tp = field.type
        null_type = '' if field.nullable == True else 'not null'

        if convert := get_type_arrow(tp):
           ty_out = convert
        else:
           ty_out = map_typs[tp]

        list_types.add(f'{col} {ty_out} {null_type}')
    
    stmt = f'CREATE TABLE #{tbl} (\n'
    stmt += textwrap.indent(',\n'.join(map(str.strip,list_types)), '   ')
    stmt += '\n)'

    return stmt


# EXPORTANDO UMA TABELA
if __name__ == '__main__':
    with raw_sql(DRIVER, autocommit=False) as cursor:
        cursor.fast_executemany = True
        