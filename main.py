from src.api import pull_data

# from src.parse_settings import get_settings
from src.db import insert_data, append_data

# from src.functions import create_dataframe
from src.log import set_logging


def mainscript(refresh=True, upload=False):

    if refresh:
        df = pull_data()

    if upload:
        # insert_data(df)
        append_data(df)


if __name__ == "__main__":
    set_logging()
    mainscript(refresh=True, upload=True)
