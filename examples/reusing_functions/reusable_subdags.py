import pandas as pd

from hamilton.function_modifiers import extract_columns, reuse, reuse_functions, value


@extract_columns("timestamp", "user_id", "region")  # one of "US", "CA" (canada)
def website_interactions(random_seed: int) -> pd.DataFrame:
    """Gives event-driven data with a series

    :return: Some mock event data.
    """
    # TODO -- implement some random data
    data = [
        ("20220901-14:00:00", 1, "US"),
        ("20220901-18:30:00", 2, "US"),
        ("20220901-19:00:00", 1, "US"),
        ("20220902-08:00:00", 1, "US"),
        ("20220903-16:00:00", 1, "US"),
        ("20220907-13:00:00", 1, "US"),
        ("20220910-14:00:00", 1, "US"),
        ("20220911-12:00:00", 1, "US"),
        ("20220914-11:00:00", 1, "US"),
        ("20220915-07:30:00", 1, "US"),
        ("20220916-06:00:00", 1, "US"),
        ("20220917-16:00:00", 1, "US"),
        ("20220920-17:00:00", 1, "US"),
        ("20220922-09:30:00", 1, "US"),
        ("20220922-10:00:00", 1, "US"),
        ("20220924-07:00:00", 1, "US"),
        ("20220924-08:00:00", 1, "US"),
        ("20220925-21:00:00", 1, "US"),
        ("20220926-15:30:00", 1, "US"),
    ]
    return pd.DataFrame(data)


def _validate_grain(grain: str):
    assert grain in ["day", "week"]


def interactions_filtered(filtered_interactions: pd.DataFrame, region: str) -> pd.DataFrame:
    pass


def unique_users(filtered_interactions: pd.DataFrame, grain: str) -> pd.Series:
    """Gives the number of shares traded by the frequency"""
    assert grain in ["day", "week", "month"]
    return ...


@reuse_functions(
    with_inputs={"grain": value("day")},
    namespace="daily_users",
    outputs={"unique_users": "unique_users_daily"},
    with_config={"region": "US"},
    load_from=[unique_users, interactions_filtered],
)
def daily_user_data_US() -> reuse.MultiOutput(unique_users_daily_US=pd.Series):  # noqa: F821
    pass


@reuse_functions(
    with_inputs={"grain": value("day")},
    namespace="daily_users",
    outputs={"unique_users": "unique_users_daily"},
    with_config={"region": "CA"},
    load_from=[unique_users, interactions_filtered],
)
def daily_user_data_CA() -> reuse.MultiOutput(unique_users_daily_CA=pd.Series):  # noqa: F821
    pass
