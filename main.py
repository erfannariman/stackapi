import logging
from src.api import pull_data
from src.parse_settings import get_settings
from src.log import set_logging
from src.db import auth_azure, export_data
from src.scraper import run_scraper

settings = get_settings("settings.yml")


def run():
    if settings["get_pandas_methods"]:
        run_scraper()
        logging.info("finished scraping pandas methods")

    if settings["run_api"]:
        dfs = pull_data()
        for name, df in dfs.items():
            export_data(df=df, name=name, db_engine=auth_azure())
        logging.info("finished uploading!")


if __name__ == "__main__":
    set_logging()
    run()
