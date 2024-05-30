from arrow_mssql.export import to_parquet
from arrow_mssql.iport import write_parquet, write_csv
from time import perf_counter
import glob
import os

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
    
    to_parquet(
        DRIVER,
        'SELECT TOP 300000 * FROM COSMOSPDP.DBO.RECUPERAVEL_COLETA_DET',   
        path='teste.parquet',
        database='COSMOSPDP',
        schema='dbo'    
    )       