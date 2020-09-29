import requests
import logging
from src.functions import MakeDataFrame, ParseResponse
from src.parse_settings import get_settings
import os

settings = get_settings("settings.yml")
BASE_URL = "https://api.stackexchange.com/2.2/questions"


def pull_data():
    """
    Convert json retrieved from API to pandas dataframes.
    :return: dictionary with create_answers and questions dataframes.
    """
    logging.info("pulling data from Stack API...")

    answers, questions = get_data()

    data = MakeDataFrame(questions, answers)

    for name, df in data.dfs.items():
        logging.info(f"imported {name} with {len(df)} records!")

    return data


def get_data():
    """
    Retrieve data from certain tag from stackexchange
    :return: json with create_answers and questions
    """
    has_more = True
    page = 0
    all_questions = list()
    all_answers = list()

    while has_more:
        page += 1
        json = request_data(page)
        answers, questions = ParseResponse.parse_json(json)
        all_answers = all_answers + answers
        all_questions = all_questions + questions
        has_more = json["has_more"]
        logging.info(
            f"pulled {len(all_questions)} questions and {len(all_answers)} create_answers..."
        )

        if page == 2:
            has_more = False

    return all_answers, all_questions


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
