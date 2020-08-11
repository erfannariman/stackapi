import os

def auth_azure():

    uid = os.environ.get('SQL_YELLOWSTACKS_DEV_USER')
    password = os.environ.get('SQL_YELLOWSTACKS_DEV_PW')
    server = 'yellowstacks-dev.database.windows.net'
    database = 'landing'
    driver = 'ODBC Driver 17 for SQL Server'

    connectionstring = f'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'

    return connectionstring

