from arrow_mssql.export import to_csv

DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)

# EXPORTANDO UMA TABELA

if __name__ == '__main__':
    to_csv(
        DRIVER,
        "SELECT TOP 10 * FROM PRODUTO_MESTRE WITH(NOLOCK)",
        database='cosmos_v14b',
        schema='dbo',
        query=True,
        path='teste.csv'
    )