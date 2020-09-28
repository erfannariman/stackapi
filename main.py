from src.api import pull_data
from src.log import set_logging
from src.parse_settings import get_settings
from src.scraper import run_scraper
from src.db import export_dfs_to_azure
from src.functions import MethodCounts
from datetime import datetime
import logging

settings = get_settings("settings.yml")


def run():
    if settings["get_pandas_methods"]:
        dfs = run_scraper()
        export_dfs_to_azure(dfs, method="replace")

    if settings["run_api"]:
        dfs = pull_data()
        export_dfs_to_azure(dfs, method=settings['method'])

    if settings["method_count"]:
        method_counts = MethodCounts()
        method_counts.method_counts_to_db()


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script method_usage at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    run()
