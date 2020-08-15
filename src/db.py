import os
import logging
import pandas as pd
from src.parse_settings import get_settings

settings = get_settings("settings.yml")
METHOD = settings["method"]
SCHEMA = settings["schema"]


def auth_azure():

    uid = os.environ.get("SQL_YELLOWSTACKS_DEV_USER")
    password = os.environ.get("SQL_YELLOWSTACKS_DEV_PW")
    server = os.environ.get("SQL_YELLOWSTACKS_DEV_SERVER")
    database = os.environ.get("SQL_YELLOWSTACKS_DEV_DB")
    driver = "ODBC Driver 17 for SQL Server"

    connection_string = f"mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}"

    return connection_string


def read_from_database(name, db_engine, schema):
    """
    :param name: name of table.
    :param db_engine: connection string to database.
    :param schema: schema name in database
    :return: list of id's that currently are stored in the Azure SQL database.
    """

    query = f"SELECT DISTINCT {name}_id FROM {schema}.pandas_{name};"
    id_list = pd.read_sql_query(query, con=db_engine)
    logging.info(f"currently {len(id_list)} {name}s in database")

    return id_list


def determine_new_table(df, name, db_engine, schema):
    """
    :param df: the extracted data set with questions/answers from the stack exchange api.
    :param name: name of table.
    :param db_engine: connection string to database.
    :param schema: schema name in database
    :return: dataset with only new questions/answers that are not already stored in the database.
    """

    id_list_db = read_from_database(name, db_engine, schema)
    df = df[~df[f"{name}_id"].isin(id_list_db[f"{name}_id"])].copy()

    logging.info(f"{len(df)} new {name}s!")

    return df


def export_data(df, name, method):
    """
    Write data to database
    :param df: data set with NEW records (either questions or answers).
    :param name: name of table.
    :param db_engine: connection string to database.
    :return: None
    """
    db_engine = auth_azure()

    if method == "append":
        df = determine_new_table(df, name, db_engine, SCHEMA)

    logging.info(
        f"executing {method} on table '{name}' ({len(df)} records) to Azure"
    )
    df["date_added"] = pd.to_datetime("now")
    df.to_sql(
        name=f"pandas_{name}",
        con=auth_azure(),
        if_exists=method,
        schema=SCHEMA,
        index=False,
    )
    logging.info(f"finished executing {method}!")


def export_dfs_to_azure(dfs, method):
    """

    :param dfs: dictionary of dataframe names (keys) and dataframes (values)
    :return: uploads the dataframes with the given names to Azure SQL Server.
    """

    for name, df in dfs.items():
        export_data(df=df, name=name, method=method)
    logging.info("finished upload!")
