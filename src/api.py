import requests
import logging
from src.functions import MakeDataFrame
from src.parse_settings import get_settings
import os

settings = get_settings("settings.yml")
BASE_URL = "https://api.stackexchange.com/2.2/questions"


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


def get_data():
    """
    Retrieve data from certain tag from stackexchange
    :return: json with answers and questions
    """
    has_more = True
    page = 0
    data = []

    while has_more:
        page += 1
        json = request_data(page)
        data.append(json)
        has_more = json["has_more"]
        logging.info(f"pulled {sum([len(x['items']) for x in data])} records...")

    return data


def request_data(page):

    params = set_parameters(page)
    request = requests.get(BASE_URL, params=params)
    json = request.json()

    return json


def set_parameters(page):
    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": settings["module"],
        "site": "stackoverflow",
        "filter": "!17vW0QCdaXujT5YQflD_sS_8ZjSYTT51wvIeyoJW0JSm4D",
        "pagesize": "100",
        "page": str(page),
    }

    if settings["use_apikey"]:
        params.update(
            {
                "access_token": os.environ.get("STACKEX_ACCESS_TOKEN"),
                "key": os.environ.get("STACKEX_KEY"),
            }
        )

    return params
