from arrow_mssql.iport import write_parquet
from datetime import datetime


DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)


# EXPORTANDO UMA TABELA
if __name__ == '__main__':
    raiz = (
        r'C:\Users\26834\OneDrive - paguemenos.com.br'
        r'\Ferramentas\App\PYTHON\projeto_recuperavel_parquet'
        r'\coletas.parquet'
    )
    
    print(datetime.now())
    with write_parquet(DRIVER, '##teste', path=raiz) as cur:
        cur.execute('SELECT count(*) FROM ##teste')
        print(cur.fetchone())
    print(datetime.now())
           