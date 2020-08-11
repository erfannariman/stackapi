import logging

from src.api import get_data
# from src.parse_settings import get_settings
from src.db import auth_azure
# from src.functions import create_dataframe
from src.functions import MakeDataFrame
from src.log import set_logging


def pull_data():
    logging.info('pulling data from Stack API...')

    json = get_data()
    mkdf = MakeDataFrame(json)
    df = mkdf.create_dataframe()
    logging.info(f'imported {len(df)} records!')

    return df


def upload_data(df):
    logging.info(f'uploading {len(df)} records to Azure...')
    engn = auth_azure()
    df.to_sql(name='pandas', con=engn, if_exists='replace', schema='method_usage', index=False)
    logging.info('finished uploading!')


def mainscript():
    df = pull_data()
    upload_data(df)


if __name__ == '__main__':
    set_logging()
    mainscript()
