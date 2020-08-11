import requests

BASE_URL = "https://api.stackexchange.com/2.2/questions"


def get_data():

    # filters:
    # !OfYUQY)-)L6xn6G_V_fwGSi-NmRjALbnnHtVSJYveQq
    # !17vW0QCdaXujT5YQfkhCM0wsVaCgMh(y41XVwBAXDRaS8D
    params = {
      "order": "desc",
      "sort": "creation",
      "tagged": "pandas",
      "site": "stackoverflow",
      "filter": "!17vW0QCdaXujT5YQfkhCM0wsVaCgMh(y41XVwBAXDRaS8D"
      }

    request = requests.get(BASE_URL, params=params)
    json = request.json()

    return json
