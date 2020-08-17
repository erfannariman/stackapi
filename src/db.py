import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from src.parse_settings import get_settings

settings = get_settings("settings.yml")
METHOD = settings["method"]
SCHEMA = settings["schema"]


def auth_azure():
    """
    :return: creates connection string (from environment variables!) for azure database
    """

    uid = os.environ.get("SQL_YELLOWSTACKS_DEV_USER")
    password = os.environ.get("SQL_YELLOWSTACKS_DEV_PW")
    server = os.environ.get("SQL_YELLOWSTACKS_DEV_SERVER")
    database = os.environ.get("SQL_YELLOWSTACKS_DEV_DB")
    driver = "ODBC Driver 17 for SQL Server"

    connection_string = (
        f"mssql+pyodbc://{uid}:{password}@" f"{server}:1433/{database}?driver={driver}"
    )

    return connection_string


def export_data(df, name, method):
    """
    Write data to database
    :param df: data set with NEW records (either questions or answers).
    :param name: name of table.
    :param method: append or replace data in database.
    :return: None
    """
    db_engine = auth_azure()

    if method == "append":
        df = delete_current_records(df, name)

    logging.info(f"executing {method} on table '{name}' ({len(df)} records) to Azure")
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
    :param method: append or replace data in database.
    :return: uploads the dataframes with the given names to Azure SQL Server.
    """

    for name, df in dfs.items():
        export_data(df=df, name=name, method=method)
    logging.info("finished upload!")


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

    current_db = read_from_database(name, db_engine=auth_azure(), schema="method_usage")
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
    sql_stmt = f"DELETE FROM method_usage.pandas_{name} WHERE {name}_id IN ({sql_list})"
    logging.info(f"{len(del_list)} {name} in delete statement")

    return sql_stmt


def execute_stmt(stmt):
    """
    :param stmt: SQL statement to be executed
    :return: executes the statment
    """
    engn = create_engine(auth_azure())

    with engn.connect() as con:
        rs = con.execute(stmt)

    return rs


def export_data(df, name, method):
    """
    Write data to database
    :param df: data set with NEW records (either questions or answers).
    :param name: name of table.
    :param db_engine: connection string to database.
    :return: None
    """

    if method == "append":
        delete_current_records(df, name)

    logging.info(f"executing {method} on table '{name}' ({len(df)} records) to Azure")
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
