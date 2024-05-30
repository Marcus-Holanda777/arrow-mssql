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
    
    for file in glob.iglob('C://python_dia_dia/ressarcimento_hoje/*.parquet'):
        name, exten = os.path.basename(file).split('.')
        print(f'Tabela: {name}')

        n1 = perf_counter()
        with write_parquet(
            DRIVER_DEGUG,
            name,
            path=file,
            override=True
        ) as cur:
                
            diff = perf_counter() - n1
            print(f'{diff:.4f}')
            
            cur.execute(f'SELECT COUNT(*) FROM {name} WITH(NOLOCK)')
            [(schema,)] = cur.fetchall()
            print(schema)