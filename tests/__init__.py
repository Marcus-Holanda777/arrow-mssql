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

    with write_csv(
        DRIVER_DEGUG, 
        '##ANALISE_BASE',
        path='c:/arquivos/data.csv',
        column_types={'Chave_NF': pa.string()}
    ) as c:
        
        c.execute("SELECT * FROM ##ANALISE_BASE")
        print(c.fetchone())