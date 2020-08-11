import logging
from src.api import pull_data
from src.parse_settings import get_settings
from src.log import set_logging
from src.db import auth_azure, export_data


settings = get_settings("settings.yml")
REFRESH = settings["refresh"]
UPLOAD = settings["upload"]
METHOD = settings["method"]
SCHEMA = settings["schema"]


def run():
    dfs = pull_data()
    if UPLOAD:
        for name, df in dfs.items():
            export_data(
                df=df, name=name, db_engine=auth_azure(), method=METHOD, schema=SCHEMA
            )
    logging.info("finished uploading!")


if __name__ == "__main__":
    set_logging()
    run()
