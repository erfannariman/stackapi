from src.ssh import server
from src.api import get_data
# from src.functions import create_dataframe
from src.functions import MakeDataFrame
from src.parse_settings import get_settings
from src.db import create_db_engine

SETTINGS = get_settings('settings.yml')
DB_USER = SETTINGS['db_user']
DB_PW = SETTINGS['db_pw']
DB_NAME = SETTINGS['db_name']

# server.daemon_forward_servers = True
server.start()

engine = create_db_engine(DB_USER, DB_PW, DB_NAME)

json = get_data()
mkdf = MakeDataFrame(json)
df = mkdf.create_dataframe()
df.to_sql(name='test', con=engine, if_exists='replace', schema='Test_DB', index=False)
engine.dispose()

server.stop()

print('finished')
