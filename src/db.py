from sqlalchemy import create_engine
from src.ssh import server


def create_db_engine(user, pw, db_name):
    engine = create_engine(
        f'mysql+mysqldb://{user}:{pw}@127.0.0.1:{server.local_bind_port}',
        echo=True
    )

    engine.execute('CREATE DATABASE IF NOT EXISTS Test_DB;')
    engine.dispose()

    engine = create_engine(
        f'mysql+mysqldb://{user}:{pw}@127.0.0.1:{server.local_bind_port}/{db_name}',
        encoding='utf-8',
        echo=True
    )

    return engine
