from arrow_mssql.export import to_parquet
from arrow_mssql.iport import write_parquet, write_csv
from time import perf_counter
import glob
import os
import pyarrow as pa


DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)

DRIVER_DEGUG = (
    'Driver={ODBC Driver 17 for Sql Server};'
    'Server=localhost;'
    'Database=testes;'
    'Uid=user_teste;'
    'Pwd=abcd.1234;'
)


# EXPORTANDO UMA TABELA
if __name__ == '__main__':

    to_parquet(DRIVER, 
        """select dscXml from dbnfe.dbo.tbNfeXml with(nolock)
           where dthGravacao BETWEEN '2024-08-01 00:00:00.000' and '2024-08-01 23:59:59.999'
        """, 
        database='cosmos_v14b', 
        path='vendas.parquet',
        chunk_size=1_000
    )