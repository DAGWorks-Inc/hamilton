import daft
from daft import DataType, col, udf

from hamilton.function_modifiers import extract_columns


# @extract_columns("signups", "spend")
def base_df(base_df_location: str) -> daft.DataFrame:
    """Loads base dataframe of data.

    :param base_df_location: just showing that we could load this from a file...
    :return: a Polars dataframe
    """
    return daft.from_pydict(
        {
            "signups": [1, 10, 50, 100, 200, 400],
            "spend": [10, 10, 20, 40, 40, 50],
        }
    )


def avg_3wk_spend(base_df: daft.DataFrame) -> daft.DataFrame:
    """Computes rolling 3 week average spend."""
    # return base_df.with_column("avg_3wk_spend", (col("spend").agg("sum")), "sum")]
    # return spend.rolling_mean(3)


def spend_per_signup(base_df: daft.DataFrame) -> daft.DataFrame:
    """Computes cost per signup in relation to spend."""

    return base_df.with_column("spend_per_signup", base_df["spend"] / base_df["signups"])


def spend_mean(base_df: daft.DataFrame) -> daft.DataFrame:
    """Shows function creating a scalar. In this case it computes the mean of the entire column."""
    return base_df.with_column("spend_mean", base_df.agg([(col("spend"), "sum")]))


def spend_zero_mean(spend_mean: daft.DataFrame) -> daft.DataFrame:
    """Shows function that takes a scalar. In this case to zero mean spend."""
    return spend_mean.with_column("spend_zero_mean", spend_mean["spend"] - spend_mean["spend_mean"])


def spend_std_dev(base_df: daft.DataFrame) -> daft.DataFrame:
    """Computes the standard deviation of the spend column."""
    return base_df.with_column("spend_std_dev", base_df["spend"].std())


def spend_zero_mean_unit_variance(
    spend_zero_mean: daft.DataFrame, spend_std_dev: daft.DataFrame
) -> daft.DataFrame:
    """Shows one way to make spend have zero mean and unit variance."""
    return spend_zero_mean.with_column(
        "spend_zero_mean_unit_variance",
        spend_zero_mean["spend_zero_mean"] / spend_zero_mean["spend_std_dev"],
    )
