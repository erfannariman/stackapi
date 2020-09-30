import os
import logging
import pandas as pd
from src.parse_settings import get_settings
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime

try:
    settings = get_settings("settings.yml")
except FileNotFoundError:
    settings = get_settings("settings_template.yml")
METHOD = settings["method"]
SCHEMA = settings["schema"]


def auth_azure():

    uid = os.environ.get("SQL_YELLOWSTACKS_DEV_USER")
    password = os.environ.get("SQL_YELLOWSTACKS_DEV_PW")
    server = os.environ.get("SQL_YELLOWSTACKS_DEV_SERVER")
    database = os.environ.get("SQL_YELLOWSTACKS_DEV_DB")
    driver = "ODBC Driver 17 for SQL Server"

    connection_string = (
        f"mssql+pyodbc://{uid}:{password}@" f"{server}:1433/{database}?driver={driver}"
    )

    engine = create_engine(connection_string)

    return engine


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


def delete_current_records(df, name):
    """
    :param df: new records
    :param name: name of the table
    :return: executes a delete statement in Azure SQL for the new records.
    """

    del_list = get_overlapping_records(df, name)
    stmt = create_sql_delete_stmt(del_list, name)

    if len(del_list):
        execute_stmt(stmt)
    else:
        logging.info("skip deleting. no records in delete statement")


def get_overlapping_records(df, name):
    """
    :param df: the dataframe containing new records
    :param name: the name of the table
    :return:  a list of records that are overlapping
    """

    current_db = read_from_database(name, db_engine=auth_azure(), schema=SCHEMA)
    overlapping_records = current_db[current_db[f"{name}_id"].isin(df[f"{name}_id"])]
    del_list = overlapping_records.astype(str)[f"{name}_id"].to_list()

    return del_list


def create_sql_delete_stmt(del_list, name):
    """
    :param del_list: list of records that need to be formatted in SQL delete statement.
    :param name: the name of the table
    :return: SQL statement for deleting the specific records
    """
    sql_list = ", ".join(del_list)
    sql_stmt = f"DELETE FROM {SCHEMA}.pandas_{name} WHERE {name}_id IN ({sql_list})"
    logging.info(f"{len(del_list)} {name} in delete statement")

    return sql_stmt


def execute_stmt(stmt):
    """
    :param stmt: SQL statement to be executed
    :return: executes the statment
    """
    engn = auth_azure()

    with engn.connect() as con:
        rs = con.execute(stmt)

    return rs


def determine_new_table(df, name, db_engine, schema):
    """
    :param df: extracted data set from the stack exchange api.
    :param name: name of table.
    :param db_engine: connection string to database.
    :param schema: schema name in database
    :return: df with only new data that's not already stored in the database.
    """

    id_list_db = read_from_database(name, db_engine, schema)
    df = df[~df[f"{name}_id"].isin(id_list_db[f"{name}_id"])].copy()

    logging.info(f"{len(df)} new {name}s!")

    return df


def execute_sql_file(sql_file):
    engine = auth_azure()
    connection = engine.connect()

    file = open(os.path.join("src", "models", sql_file))
    query = text(file.read())
    connection.execute(query)


def export_data(df, name, method):
    """
    Write data to database
    :param df: data set with NEW records (either questions or create_answers).
    :param name: name of table.
    :param method: append or replace data in database.
    :return: None
    """
    if method == "append":
        delete_current_records(df, name)

    if not len(df):
        return logging.info(f"skipped upload of table {name}.(0 records)")

    logging.info(f"executing {method} on table '{name}' ({len(df)} records) to Azure")
    df["date_added"] = datetime.now()

    df.to_sql(
        name=f"pandas_{name}", con=auth_azure(), if_exists='append', schema=SCHEMA, index=False,
    )
    logging.info(f"finished executing {method}!")


def export_dfs_to_azure(dfs, method):
    """
    :param dfs: dictionary of dataframe names (keys) and dataframes (values)
    :param method: append or replace data in database.
    :return: uploads the dataframes with the given names to Azure SQL Server.
    """
    if method == "replace":
        sql_files = os.listdir(os.path.join("src", "models"))
        for sql_file in sql_files:
            logging.info(f"Executing SQL file {sql_file}")
            execute_sql_file(sql_file)

    for name, df in dfs.items():
        export_data(df=df, name=name, method=method)

    logging.info("finished upload!")


def export_dfs_to_pickles(dfs):
    for name, df in dfs.items():
        df.to_pickle(os.path.join('data', f"{name}.pkl"))
    logging.info("finished exporting to .pkl!")
