import pandas as pd
from src.db import auth_azure, execute_sql_file
from src.parse_settings import get_settings
import logging

try:
    settings = get_settings("settings.yml")
except FileNotFoundError:
    settings = get_settings("settings_template.yml")
SCHEMA = settings["schema"]
MODULE = settings["module"]


class ParseResponse:
    def __init__(self, json):
        self.json = json

    def parse_json(self):
        """
        Split json into questions part and answer part.
        The answer part is a nested JSON itself.
        :return: two lists of dictionaries
        """
        items = self["items"]
        # we use pop, because we want to
        # remove create_answers from the original dictionary
        answers = [item.pop("answers", None) for item in items]
        questions = items

        return answers, questions


class MakeDataFrame:
    def __init__(self, questions, answers):

        self.answers = answers
        self.questions = questions
        self.dfs = self.create_dataframes()

    @staticmethod
    def select_string_columns(df):
        """
        Get string columns of dataframe and exclude owner
        :param df: dataframe
        :return: list of columns
        """
        columns = df.select_dtypes(object).columns
        columns = [col for col in columns if col != "owner"]

        return columns

    @staticmethod
    def get_user_id(df, col):
        df[col] = df[col].str["user_id"]

        return df

    def create_answers(self):
        """
        Create create_answers dataframe from list of dictionaries
        :param answers: list of dictionaries
        :return: DataFrame
        """

        df = pd.concat([pd.DataFrame(x) for x in self.answers], ignore_index=True)
        df = self.get_user_id(df, "owner")
        date_cols = ["last_activity_date", "creation_date", "last_edit_date"]
        df[date_cols] = df[date_cols].apply(lambda x: pd.to_datetime(x, unit="s", utc=True))
        df["body"] = df.body.str.replace("<[^<]+?>", "")

        return df

    def create_questions(self):
        """
        Create questions dataframe from list of dictionaries
        :param questions: list of dictionaries
        :return: DataFrame
        """
        df = pd.DataFrame(self.questions)
        df = self.get_user_id(df, "owner")
        df["tags"] = df["tags"].str.join(", ")
        date_cols = ["last_activity_date", "creation_date", "last_edit_date"]
        df[date_cols] = df[date_cols].apply(lambda x: pd.to_datetime(x, unit="s", utc=True))
        df["body"] = df.body.str.replace("<[^<]+?>", "")

        return df
        return df

    def create_dataframes(self):
        """
        Create both create_answers, questions dataframes
        :return: create_answers dataframe, questions dataframe
        """
        dfs = {
            "answer": self.create_answers(),
            "question": self.create_questions(),
        }

        return dfs


class MethodCounts:
    def __init__(self):
        logging.info("Starting method count analysis on data")
        self.engine = auth_azure()
        # TODO: this should be done dynamically for each module
        self.tables = [
            f"{MODULE}_methods",
            f"{MODULE}_answer",
            f"{MODULE}_question",
        ]
        logging.info("Reading in tables from db for method counts")
        self.dfs = {
            table: pd.read_sql_table(table, con=self.engine, schema="method_usage")
            for table in self.tables
        }
        self.to_replace = [
            r"pd",
            r".Series",
            r".DataFrame",
            r".DatetimeIndex",
            r".Index",
            r".Timedelta",
            r".IntervalIndex",
            r".MultiIndex",
            r".CategoricalIndex",
            r".Timestamp",
            r".Period",
            r".core.groupby.GroupBy",
            r".core.groupbyGroupBy",
        ]

    def methods(self):
        methods = self.dfs[f"{MODULE}_methods"]
        methods["methods"] = methods["methods"].str.replace("|".join(self.to_replace), "")
        df_methods = methods[methods["methods"].str.len().ne(0)]
        methods = "|".join(df_methods["methods"])

        return methods

    def method_count(self, df):
        matches = df["body"].str.extractall(f"({self.methods()})")
        matches = matches[matches[0].str.startswith(".")]
        matches = matches.value_counts()
        matches = matches.reset_index(name="count").rename(columns={0: "methods"})
        matches["module"] = MODULE
        matches["date_added"] = pd.to_datetime("now", utc=True)

        return matches

    def create_method_count_tables(self):
        dfs = self.dfs
        qa = {
            "question": dfs[f"{MODULE}_question"],
            "answer": dfs[f"{MODULE}_answer"],
        }
        logging.info("Executing method count analysis on tables")
        method_counts = {name: self.method_count(df) for name, df in qa.items()}
        logging.info("Finished method count analysis")
        return method_counts

    def method_counts_to_db(self):
        dfs = self.create_method_count_tables()
        logging.info("Writing results method counts to db")
        for name, df in dfs.items():
            execute_sql_file(f"method_counts_{name}.sql")
            df.to_sql(
                name=f"method_counts_{name}",
                con=auth_azure(),
                if_exists="append",  # TODO: should this be replace or ..?
                schema=SCHEMA,
                index=False,
            )
        logging.info("Finished writing result method counts to db")
