import pandas as pd
import unique_users

from hamilton.experimental.decorators import reuse
from hamilton.experimental.decorators.reuse import reuse_functions
from hamilton.function_modifiers import value


def website_interactions() -> pd.DataFrame:
    """Gives event-driven data with a series

    :return: Some mock event data.
    """
    data = [
        ("20220901-14:00:00", 1, "US"),
        ("20220901-18:30:00", 2, "US"),
        ("20220901-19:00:00", 1, "US"),
        ("20220902-08:00:00", 3, "US"),
        ("20220903-16:00:00", 1, "US"),
        ("20220907-13:00:00", 4, "US"),
        ("20220910-14:00:00", 1, "US"),
        ("20220911-12:00:00", 3, "US"),
        ("20220914-11:00:00", 1, "US"),
        ("20220915-07:30:00", 2, "US"),
        ("20220916-06:00:00", 1, "US"),
        ("20220917-16:00:00", 2, "US"),
        ("20220920-17:00:00", 5, "US"),
        ("20220922-09:30:00", 2, "US"),
        ("20220922-10:00:00", 1, "US"),
        ("20220924-07:00:00", 6, "US"),
        ("20220924-08:00:00", 1, "US"),
        ("20220925-21:00:00", 1, "US"),
        ("20220926-15:30:00", 2, "US"),
        ("20220901-14:00:00", 7, "CA"),
        ("20220901-18:30:00", 8, "CA"),
        ("20220901-19:00:00", 9, "CA"),
        ("20220902-08:00:00", 7, "CA"),
        ("20220903-16:00:00", 10, "CA"),
        ("20220907-13:00:00", 9, "CA"),
        ("20220910-14:00:00", 8, "CA"),
        ("20220911-12:00:00", 11, "CA"),
        ("20220914-11:00:00", 12, "CA"),
        ("20220915-07:30:00", 7, "CA"),
        ("20220916-06:00:00", 9, "CA"),
        ("20220917-16:00:00", 10, "CA"),
        ("20220920-17:00:00", 7, "CA"),
        ("20220922-09:30:00", 11, "CA"),
        ("20220922-10:00:00", 8, "CA"),
        ("20220924-07:00:00", 9, "CA"),
        ("20220924-08:00:00", 10, "CA"),
        ("20220925-21:00:00", 13, "CA"),
        ("20220926-15:30:00", 14, "CA"),
    ]
    df = (
        pd.DataFrame(data, columns=["timestamp", "user_id", "region"])
        .set_index("timestamp")
        .sort_index()
    )
    df.index = pd.DatetimeIndex(df.index)
    return df


@reuse_functions(
    with_inputs={"grain": value("day"), "region": value("US")},
    namespace="daily_users_US",
    outputs={"unique_users": "unique_users_daily_US"},
    with_config={"region": "US"},
    load_from=[unique_users],
)
def daily_user_data_US() -> reuse.MultiOutput(unique_users_daily_US=pd.Series):
    pass


@reuse_functions(
    with_inputs={"grain": value("week"), "region": value("US")},
    namespace="weekly_users_US",
    outputs={"unique_users": "unique_users_weekly_US"},
    with_config={"region": "US"},
    load_from=[unique_users],
)
def weekly_user_data_US() -> reuse.MultiOutput(unique_users_weekly_US=pd.Series):
    pass


@reuse_functions(
    with_inputs={"grain": value("month"), "region": value("US")},
    namespace="monthly_users_US",
    outputs={"unique_users": "unique_users_monthly_US"},
    with_config={"region": "US"},
    load_from=[unique_users],
)
def monthly_user_data_US() -> reuse.MultiOutput(unique_users_monthly_US=pd.Series):
    pass


@reuse_functions(
    with_inputs={"grain": value("day"), "region": value("CA")},
    namespace="daily_user_data_CA",
    outputs={"unique_users": "unique_users_daily_CA"},
    with_config={"region": "CA"},
    load_from=[unique_users],
)
def daily_user_data_CA() -> reuse.MultiOutput(unique_users_daily_CA=pd.Series):
    pass


@reuse_functions(
    with_inputs={"grain": value("month"), "region": value("CA")},
    namespace="weekly_user_data_CA",
    outputs={"unique_users": "unique_users_weekly_CA"},
    with_config={"region": "CA"},
    load_from=[unique_users],
)
def weekly_user_data_CA() -> reuse.MultiOutput(unique_users_weekly_CA=pd.Series):
    pass


@reuse_functions(
    with_inputs={"grain": value("day"), "region": value("CA")},
    namespace="monthly_user_data_CA",
    outputs={"unique_users": "unique_users_monthly_CA"},
    with_config={"region": "CA"},
    load_from=[unique_users],
)
def monthly_user_data_CA() -> reuse.MultiOutput(unique_users_monthly_CA=pd.Series):
    pass
