import pandas as pd
import pyspark.pandas as ps

from hamilton.function_modifiers import extract_columns

# You could have two separate loaders:
#
# def spend(spend_location: str) -> ps.Series:
#     """Dummy function showing how to wire through loading data.
#
#     :param spend_location:
#     :return:
#     """
#     return ps.from_pandas(pd.Series([10, 10, 20, 40, 40, 50], name="spend"))
#
#
# def signups(signups_location: str) -> ps.Series:
#     """Dummy function showing how to wire through loading data.
#
#     :param signups_location:
#     :return:
#     """
#     return ps.from_pandas(pd.Series([1, 10, 50, 100, 200, 400], name="signups"))


# Or one loader where you extract its columns:
@extract_columns("spend", "signups")
def base_df(base_df_location: str) -> ps.DataFrame:
    """Dummy function showing how to wire through loading data.

    :param location:
    :return:
    """
    return ps.from_pandas(
        pd.DataFrame({"spend": [10, 10, 20, 40, 40, 50], "signups": [1, 10, 50, 100, 200, 400]})
    )
