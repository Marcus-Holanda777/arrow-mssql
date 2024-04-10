from arrow_mssql.input.datatypes import (
    get_type_arrow, 
    map_typs, 
    get_type_arrow_pyodbc, 
    map_typs_pyodbc
)
import pyarrow as pa
import textwrap


def drop_table(tbl: str) -> str:
    return f'DROP TABLE IF EXISTS {tbl}'


def create_table(
    tbl: str, 
    schema_arrow: pa.Schema
) -> str:
    
    list_types = []

    for field in schema_arrow:
        col = field.name
        tp = field.type
        null_type = '' if field.nullable == True else 'not null'

        if convert := get_type_arrow(tp):
           ty_out = convert
        else:
           ty_out = map_typs[tp]

        list_types.append(f'{col} {ty_out} {null_type}')
    
    stmt = f'CREATE TABLE {tbl} (\n'
    stmt += textwrap.indent(',\n'.join(map(str.strip,list_types)), '   ')
    stmt += '\n)'

    return stmt


def insert_setinputsizes(
    schema_arrow: pa.Schema
) -> list[tuple]:
    
    list_types = []

    for field in schema_arrow:
        tp = field.type

        if convert := get_type_arrow_pyodbc(tp):
           ty_out = convert
        else:
           ty_out = map_typs_pyodbc[tp]

        list_types.append(ty_out)

    return list_types


def insert_table(
    tbl: str, 
    schema_arrow: pa.Schema
) -> str:

    cols = [*map(lambda f: f.name, schema_arrow)]

    values = f"INSERT INTO {tbl} ({', '.join(cols)})"
    values += ' VALUES ('
    values += '?, ' * len(cols)
    values = values[:-2] + ')'

    return values