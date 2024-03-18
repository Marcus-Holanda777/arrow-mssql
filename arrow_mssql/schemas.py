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
import sqlglot as sg
import sqlglot.expressions as sge
from .utils import (
    is_query,
    rename_col
)


def schema_query_or_table(
    driver: str,
    name: str,
    database: str,
    schema: str
) -> list[tuple]:
    
    query = is_query(name)

    if query:
       name = sge.convert(str(name)).sql('tsql')
       stmt = dedent(
           f'''
           EXEC sp_describe_first_result_set @tsql = N{name}
           '''
       )

    else:
        where = [
            sg.column("table_name").eq(sge.convert(name)),
            sg.column("table_schema").eq(sge.convert(schema))
        ]

        stmt = (
            sg.select(
                "column_name",
                "data_type",
                "is_nullable",
                "numeric_precision",
                "numeric_scale",
                "datetime_precision"
            )
            .from_(
                sg.table(
                    'columns',
                    db='information_schema',
                    catalog=database
                )
            )
            .where(*where)
            .order_by("ordinal_position")
            .sql('tsql')
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
    schema: str = 'dbo'
) -> Callable:

    
    rst = schema_query_or_table(
        driver,
        name,
        database,
        schema
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
                rename_col(cols), 
                dtype, 
                nullable= is_null == 'YES'
            )
            schema.append(field)
            
        if retorno == 'st':
            return pa.struct(schema)        
        return pa.schema(schema)
    
    return tipo