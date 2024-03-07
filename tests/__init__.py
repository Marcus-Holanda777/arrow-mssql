from arrow_mssql.export import to_parquet
from arrow_mssql.connector import raw_sql
from arrow_mssql.datatypes import UUID_TYPE_ARROW
from arrow_mssql.schemas import get_schema
import uuid


DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=cosmos;'
    'Database=cosmos_v14b;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)


if __name__ == '__main__':
    import pyarrow as pa
    
    def is_valid_uuid(val):
        try:
            uuid.UUID(str(val), version=4)
            return True
        except ValueError:
            return False

    query_s = '''
        SELECT TOP 10000 * FROM CONFERENCIAFL.PDP.CONFERENCIA
    '''

    sh = get_schema(
        DRIVER, 
        query_s, 
        database='CONFERENCIAFL', 
        schema='pdp',
        query=True
    )('sh')

    with raw_sql(DRIVER) as cursor:    
        stmt = cursor.execute(query_s).fetchall()
        data = []

        for c in range(len(sh)):
            ver_tipo = stmt[0][c]

            if is_valid_uuid(ver_tipo):
                arr = pa.array([row[c].bytes for row in stmt], pa.binary(16))
                arr = pa.ExtensionArray.from_storage(UUID_TYPE_ARROW, arr)
            else:
                arr = pa.array([row[c] for row in stmt])

            data.append(arr)

        tbl = pa.Table.from_arrays(data, schema=sh)