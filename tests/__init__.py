from arrow_mssql.export import to_csv, to_parquet

DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)

# EXPORTANDO UMA TABELA
if __name__ == '__main__':
    to_parquet(
        DRIVER,
        'DEPOSITO',
        database='cosmos_v14b',
        schema='dbo',
        path='teste.parquet'
    )

    to_csv(
        DRIVER,
        'DEPOSITO',
        database='cosmos_v14b',
        schema='dbo',
        path='teste.csv'
    )