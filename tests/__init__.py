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

    to_parquet(
        DRIVER,
        "produto_mestre",
        path='teste.parquet',
        database='cosmos_v14b'
    )

    diff = perf_counter() - n1
    print(f'{diff:.4f}')
    
    n1 = perf_counter()
    with write_parquet(
        DRIVER_DEGUG,
        'produto_mestre',
        path='teste.parquet',
        override=True,
        limit=5000
    ) as cur:
        
        diff = perf_counter() - n1
        print(f'{diff:.4f}')
        
        cur.execute('SELECT COUNT(*) FROM produto_mestre WITH(NOLOCK)')
        [(schema,)] = cur.fetchall()
        print(schema)