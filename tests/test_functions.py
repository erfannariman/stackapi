import pandas as pd
import pandas.testing as tm

from src.functions import MakeDataFrame


make_df = MakeDataFrame


def test_select_string_columns():
    df = pd.DataFrame(
        {
            "A": list("abc"),
            "B": [1, 2, 3],
            "C": pd.date_range("2020-01-01", "2020-01-03", freq="D"),
            "D": pd.Categorical([10, 20, 30]),
            "owner": list("def"),
        }
    )

    expected = ["A"]
    result = make_df.select_string_columns(df)
    assert expected == result


def test_get_user_id():
    df = pd.DataFrame({"owner": [{"user_id": 123}, {"user_id": 456}, {"user_id": 789}]})
    expected = pd.DataFrame({"owner": [123, 456, 789]})
    result = make_df.get_user_id(df, "owner")
    tm.assert_frame_equal(expected, result)
