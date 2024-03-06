from arrow_mssql.export import to_parquet

DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)


if __name__ == '__main__':
    to_parquet(
        DRIVER,
        'DEPOSITO',
        path='incineracao.parquet',
        database='COSMOS_V14B'
    )