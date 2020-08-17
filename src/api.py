import requests
import logging
from src.functions import MakeDataFrame
from src.parse_settings import get_settings

tag = get_settings("settings.yml")["module"]
BASE_URL = "https://api.stackexchange.com/2.2/questions"


def get_data():
    """
    Retrieve data from certain tag from stackexchange
    :return: json with answers and questions
    """

    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": tag,
        "site": "stackoverflow",
        "filter": "!17vW0QCdaXujT5YQflD_sS_8ZjSYTT51wvIeyoJW0JSm4D",
    }

    request = requests.get(BASE_URL, params=params)
    json = request.json()

    return json


def pull_data():
    """
    Convert json retrieved from API to pandas dataframes.
    :return: dictionary with answers and questions dataframes.
    """
    logging.info("pulling data from Stack API...")

    json = get_data()
    make_df = MakeDataFrame(json)
    dfs = make_df.create_dataframes()
    for name, df in dfs.items():
        logging.info(f"imported {name} with {len(df)} records!")

    return dfs
