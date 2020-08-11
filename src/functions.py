import pandas as pd


class MakeDataFrame:
    def __init__(self, json):
        self.json = json
        self.df = pd.DataFrame(json["items"])
        self.cols = self.df.select_dtypes(object).columns
        self.cols = [col for col in self.cols if col != "owner"]

    def clean_up(self):
        self.df["tags"] = "pandas"
        self.df["owner"] = self.df["owner"].str["user_id"]
        self.df = self.df.rename(columns={"owner": "user_id"})
        return self.df

    def string_encode(self):
        self.df[self.cols] = self.df[self.cols].apply(
            lambda x: x.str.encode(encoding="ascii", errors="replace")
        )
        return self.df

    def string_decode(self):
        cols = self.df.select_dtypes(object).columns
        self.df[cols] = self.df[cols].apply(
            lambda x: x.str.decode(encoding="ascii", errors="replace")
        )
        return self.df

    def create_dataframe(self):
        self.df = self.clean_up()
        self.df = self.string_encode()
        self.df = self.string_decode()

        return self.df
