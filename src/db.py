import os
import logging
import pandas as pd


def auth_azure():

    uid = os.environ.get("SQL_YELLOWSTACKS_DEV_USER")
    password = os.environ.get("SQL_YELLOWSTACKS_DEV_PW")
    server = "yellowstacks-dev.database.windows.net"
    database = "landing"
    driver = "ODBC Driver 17 for SQL Server"

    connection_string = (
        f"mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}"
    )

    return connection_string


def read_from_database(name, db_engine, schema):
    """
    :param name: name of table.
    :param db_engine: connection string to database
    :param schema: schema name in database
    :return: list of question_id's that currently are stored in the Azure SQL database.
    """

    query = f"SELECT DISTINCT {name}_id FROM {schema}.pandas_{name};"
    id_list = pd.read_sql_query(query, con=db_engine)
    logging.info(f"currently {len(id_list)} questions in database")

    return id_list


def determine_new_table(df, name, db_engine, schema):
    """
    :param df: the extracted data set with questions from the stack exchange api.
    :param name: name of table.
    :param db_engine: connection string to database.
    :param schema: schema name in database
    :return: datas et with only new questions that are not already stored in the database.
    """

    id_list_db = read_from_database(name, db_engine, schema)
    df = df[~df[f"{name}_id"].isin(id_list_db[f"{name}_id"])].copy()

    logging.info(f"{len(df)} new questions!")

    return df


def export_data(df, name, db_engine, schema, method="append"):
    """
    Write data to database
    :param df: data set with NEW questions.
    :param name: name of table.
    :param db_engine: connection string to database.
    :param schema: schema name in database
    :param method: should table be replaced or new data appended, choices [replace, append]
    :return: None
    """

    if method == "append":
        df = determine_new_table(df, name, db_engine, schema)

    logging.info(f"doing {method} for table {name} with {len(df)} records to Azure...")
    df["date_added"] = pd.to_datetime("now")
    df.to_sql(
        name=f"pandas_{name}",
        con=db_engine,
        if_exists=method,
        schema=schema,
        index=False,
    )
    logging.info(f"finished doing {method}!")
