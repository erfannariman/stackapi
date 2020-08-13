from src.api import pull_data
from src.log import set_logging
from src.parse_settings import get_settings
from src.scraper import run_scraper
from src.db import export_dfs_to_azure

settings = get_settings("settings.yml")


def run():
    if settings["get_pandas_methods"]:
        dfs = run_scraper()
        export_dfs_to_azure(dfs, method = 'replace')

    if settings["run_api"]:
        dfs = pull_data()
        export_dfs_to_azure(dfs, method = 'append') # in dit geval is de yaml niet echt handig: de method variabele is voor de scraper en de api kennelijk anders


if __name__ == "__main__":
    set_logging()
    run()
