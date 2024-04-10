from arrow_mssql.iport import write_parquet


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
        r'\prodcd.parquet'
    )
    
    with write_parquet(DRIVER, '##teste', path=raiz) as cur:
        cur.execute('''
            SELECT top 1000 *
            FROM ##teste WITH(NOLOCK)
        ''')
    
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        print(rows)
           