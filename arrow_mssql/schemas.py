import pyarrow as pa
from .connector import raw_sql
from textwrap import dedent
from typing import (
    Any, 
    Literal, 
    Callable
)
from .datatypes import map_typs
from operator import itemgetter


def schema_query_or_table(
    driver: str,
    name: str,
    database: str,
    query: bool
) -> list[tuple]:
    
    if query:
       import re

       comp = re.compile(r"'")
       name = comp.sub(r"''", name)

       stmt = dedent(
           f'''
           EXEC sp_describe_first_result_set @tsql = N'{name}'
           '''
       )

    else:
        stmt = dedent(
            f'''
            select 
                column_name,
                data_type,
                is_nullable,
                numeric_precision,
                numeric_scale,
                datetime_precision
            from {database}.information_schema.columns
            where table_name = '{name}'
            '''
        )

    with raw_sql(driver) as cur:
        rst = cur.execute(stmt).fetchall()
    
    if query:

        def alter_tuple(row):
            row = list(row)
            tipo = row[1]

            if '(' in tipo:
                row[1] = tipo[:tipo.find('(')]

            row[2] = 'YES' if row[2] else 'NO'
            return tuple(row)

        filtro = itemgetter(2, 5, 3, 7, 8, 8)
        rst = [
               *map(
                   alter_tuple, 
                   map(
                       filtro, 
                       rst
                )
            )
        ]
    
    return rst
    

def get_schema(
    driver: str,
    name: str,
    database: str,
    query: bool = False
) -> Callable:

    
    rst = schema_query_or_table(
        driver,
        name,
        database,
        query
    )
    
    def tipo(
        retorno: Literal['sh', 'st', 'lt']
    ) -> Any:
        
        if retorno == 'lt':
            return rst
        
        schema = []

        for (
            cols,
            types,
            is_null,
            precision,
            scale,
            dt_precision
        ) in rst:

            dtype = map_typs[types]     
            if types in ('decimal', 'numeric'):
                dtype = dtype(precision, scale)
            
            if types == 'float':
                dtype = dtype(precision)
            
            if types in (
                'datetime',
                'datetime2', 
                'smalldatetime',
                'datetimeoffset',
                'time'
            ):
                dtype = dtype(dt_precision)

            field = pa.field(
                cols, 
                dtype, 
                nullable= is_null == 'YES'
            )
            schema.append(field)
            
        if retorno == 'st':
            return pa.struct(schema)        
        return pa.schema(schema)
    
    return tipo