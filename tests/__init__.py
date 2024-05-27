from arrow_mssql.export import to_parquet
from arrow_mssql.iport import write_parquet

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
    from time import perf_counter
    raiz = (
        r'C:\Users\26834\OneDrive - paguemenos.com.br'
        r'\Ferramentas\App\PYTHON\projeto_recuperavel_parquet'
        r'\prodcd.parquet'
    )
    
    n1 = perf_counter()
    
    with write_parquet(
        DRIVER_DEGUG, 
        'recuperavel',
        path=r'C:\Users\26834\teste_sqlserver_arrow_mssql\recuperavel.parquet',
        override=True
    ) as cur:
        
        diff = perf_counter() - n1
        print(f'{diff:.4f}')
        
        cur.execute('SELECT COUNT(*) FROM recuperavel WITH(NOLOCK)')
        [(schema,)] = cur.fetchall()
        print(schema)