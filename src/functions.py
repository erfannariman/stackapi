import pandas as pd
from src.db import auth_azure
from src.parse_settings import get_settings
import logging

try:
    settings = get_settings("settings.yml")
except FileNotFoundError:
    settings = get_settings("settings_template.yml")
SCHEMA = settings["schema"]
MODULE = settings["module"]


class MakeDataFrame:
    def __init__(self, json):
        self.json = json

    def split_json(self):
        """
        Split json into questions part and answer part.
        The answer part is a nested JSON itself.
        :return: two lists of dictionaries
        """
        items = self.json["items"]
        # we use pop, because we want to
        # remove answers from the original dictionary
        answers = [item.pop("answers", None) for item in items]
        questions = items

        return answers, questions

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

    def answers(self, answers):
        """
        Create answers dataframe from list of dictionaries
        :param answers: list of dictionaries
        :return: DataFrame
        """

        answers = pd.concat([pd.DataFrame(x) for x in answers], ignore_index=True)
        answers = self.get_user_id(answers, "owner")
        date_cols = ["last_activity_date", "creation_date", "last_edit_date"]
        answers[date_cols] = answers[date_cols].apply(
            lambda x: pd.to_datetime(x, unit="s", utc=True)
        )
        answers["body"] = answers.body.str.replace("<[^<]+?>", "")

        return answers

    def questions(self, questions):
        """
        Create questions dataframe from list of dictionaries
        :param questions: list of dictionaries
        :return: DataFrame
        """
        questions = pd.DataFrame(questions)
        questions = self.get_user_id(questions, "owner")
        questions["tags"] = questions["tags"].str.join(", ")
        date_cols = ["last_activity_date", "creation_date", "last_edit_date"]
        questions[date_cols] = questions[date_cols].apply(
            lambda x: pd.to_datetime(x, unit="s", utc=True)
        )
        questions["body"] = questions.body.str.replace("<[^<]+?>", "")

        return questions

    def create_dataframes(self):
        """
        Create both answers, questions dataframes
        :return: answers dataframe, questions dataframe
        """
        answers, questions = self.split_json()
        dfs = {
            "answer": self.answers(answers),
            "question": self.questions(questions),
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
        matches = matches.reset_index(name="count").rename(columns={0: "method"})
        matches["module"] = MODULE
        matches["module"] = pd.to_datetime("now", utc=True)

        return matches

    def create_method_count_tables(self):
        dfs = self.dfs
        qa = {
            "question": dfs[f"{MODULE}_question"],
            "answer": dfs[f"{MODULE}_answer"],
        }
        logging.info("Doing method count analysis on tables")
        method_counts = {name: self.method_count(df) for name, df in qa.items()}
        logging.info("Finished method count analysis")
        return method_counts

    def method_counts_to_db(self):
        dfs = self.create_method_count_tables()
        logging.info("Writing results method counts to db")
        for name, df in dfs.items():
            df.to_sql(
                name="method_counts",
                con=auth_azure(),
                if_exists="replace",  # TODO: should this be replace or ..?
                schema=SCHEMA,
                index=False,
            )
        logging.info("Finished writing result method counts to db")
