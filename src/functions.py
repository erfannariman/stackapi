import pandas as pd


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
