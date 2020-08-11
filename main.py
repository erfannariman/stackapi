import logging

from src.api import get_data

# from src.parse_settings import get_settings
from src.db import auth_azure

from src.functions import MakeDataFrame
from src.log import set_logging


def pull_data():
    logging.info("pulling data from Stack API...")

    json = get_data()
    make_df = MakeDataFrame(json)
    dfs = make_df.create_dataframes()
    for name, df in dfs.items():
        logging.info(f"imported {len(df)} {name} records!")

    return dfs


def upload_data(df, name, db_engine):
    logging.info(f"uploading {name} to Azure with {len(df)} records...")
    df.to_sql(
        name=f"pandas_{name}",
        con=db_engine,
        if_exists="replace",
        schema="method_usage",
        index=False,
    )


def run():
    dfs = pull_data()
    db_engine = auth_azure()
    for name, df in dfs.items():
        upload_data(df, name, db_engine)
    logging.info("finished uploading!")


if __name__ == "__main__":
    set_logging()
    run()
