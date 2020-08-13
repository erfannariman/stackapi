import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
from src.db import auth_azure
from src.parse_settings import get_settings


class Scraper:
    def __init__(self, page):
        self.base_url = 'https://pandas.pydata.org/pandas-docs/stable/reference/'
        self.page = page

    def get_page(self):
        response = requests.get(f"{self.base_url}{self.page}")
        soup = BeautifulSoup(response.text, "html.parser")

        return soup

    def get_a_tag(self):
        soup = self.get_page()
        a_tag = soup.find_all("a")

        return a_tag

    def get_links(self):
        a_tag = self.get_a_tag()

        if self.page == "index.html":
            # for links we do NOT want strings starting with "api"
            links = [
                link.get("href") for link in a_tag
                if not link.get("href").startswith("api") and "#" not in link.get("href")
            ]
            # for methods we want the strings which start with "api"
        else:
            links = [
                link.get("href") for link in a_tag
                if link.get("href").startswith("api") and "#" not in link.get("href")
            ]

        return links

    @staticmethod
    def clean_up_links(df):
        mask1 = df["links"].str.startswith((r"../", "http"))
        mask2 = df["links"].ne("")
        df = df[(~mask1) & mask2].drop_duplicates().reset_index(drop=True)

        return df

    @staticmethod
    def clean_up_methods(df):
        df['methods'] = df['methods'].str.replace(r"api/|\.html", "")
        df['methods'] = df["methods"].str.replace("pandas", "pd")

        return df

    def create_links_df(self):
        links = self.get_links()
        df = pd.DataFrame({"links": links})
        df = self.clean_up_links(df)

        return df

    def create_methods_df(self):
        links = self.get_links()
        df = pd.DataFrame({"methods": links})
        df = self.clean_up_methods(df)
        df["page"] = self.page.split(r'.')[0]

        return df


def run_scraper():
    # create links df
    logging.info("started scraping scraping index page")
    scraper = Scraper("index.html")
    links = scraper.create_links_df()

    # create methods df
    logging.info("started scraping pandas methods")
    dfs = []
    for link in links['links']:
        logging.info(f"now scraping pandas method: {link.split(r'.')[0]}")
        scraper = Scraper(link)
        df = scraper.create_methods_df()
        dfs.append(df)

    methods = pd.concat(dfs, ignore_index=True)
    dfs = {
        "links": links,
        "methods": methods,
    }

    for name, df in dfs.items():
        df.to_sql(
            name=f"pandas_{name}",
            con=auth_azure(),
            if_exists="replace",
            schema=get_settings(r"settings.yml")["schema"],
            index=False,
        )


if __name__ == "__main__":
    run_scraper()
