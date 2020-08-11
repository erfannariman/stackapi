import logging
import os

import pandas as pd


def auth_azure():

    uid = os.environ.get("SQL_YELLOWSTACKS_DEV_USER")
    password = os.environ.get("SQL_YELLOWSTACKS_DEV_PW")
    server = "yellowstacks-dev.database.windows.net"
    database = "landing"
    driver = "ODBC Driver 17 for SQL Server"

    connectionstring = (
        f"mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}"
    )

    return connectionstring


def append_data(df):
    """
    :param : dataset with NEW questions.
    :return: appending the database table with this new dataset
    """

    df = determine_new_questions(df)

    logging.info(f"appending {len(df)} records to Azure...")
    engn = auth_azure()
    df.to_sql(
        name="pandas_questions",
        con=engn,
        if_exists="append",
        schema="method_usage",
        index=False,
    )
    logging.info("finished appending!")


def insert_data(df):
    """

    :param : dataset with questions.
    :return: overwriting of the database table with this new dataset
    """

    logging.info(f"uploading {len(df)} records to Azure...")
    engn = auth_azure()
    df.to_sql(
        name="pandas_questions",
        con=engn,
        if_exists="replace",
        schema="method_usage",
        index=False,
    )
    logging.info("finished uploading!")


def questions_from_database():
    """

    :param :
    :return: list of question_id's that currently are stored in the Azure SQL database
    """

    engn = auth_azure()
    df = pd.read_sql_table("pandas_questions", con=engn, schema="method_usage")
    idlist = df.question_id.unique().tolist()
    logging.info(f"currently {len(idlist)} questions in database")
    return idlist


def determine_new_questions(df):
    """

    :param df: the extracted dataset with questions from the stack exchange api.
    :return: dataset with only new questions that are not already stored in the database.
    """

    idlist_db = questions_from_database()
    df = df[~df.question_id.isin(idlist_db)]

    logging.info(f"{len(df)} new questions!")

    return df
