import requests
import logging
from src.functions import MakeDataFrame


BASE_URL = "https://api.stackexchange.com/2.2/questions"


def get_data():

    # filters:
    # !OfYUQY)-)L6xn6G_V_fwGSi-NmRjALbnnHtVSJYveQq
    # !17vW0QCdaXujT5YQfkhCM0wsVaCgMh(y41XVwBAXDRaS8D <-- without link
    # !17vW0QCdaXujT5YQflD_sS_8ZjSYTT51wvIeyoJW0JSm4D
    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": "pandas",
        "site": "stackoverflow",
        "filter": "!17vW0QCdaXujT5YQflD_sS_8ZjSYTT51wvIeyoJW0JSm4D",
    }

    request = requests.get(BASE_URL, params=params)
    json = request.json()

    return json


def pull_data():
    logging.info("pulling data from Stack API...")

    json = get_data()
    make_df = MakeDataFrame(json)
    dfs = make_df.create_dataframes()
    for name, df in dfs.items():
        logging.info(f"imported {name} with {len(df)} records!")

    return dfs
