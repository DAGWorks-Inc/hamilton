import datetime

import pandas as pd


def due_date(start_date: datetime.datetime) -> datetime.datetime:
    """The due date is start_date + 40 weeks. Start date is the date of the expecting mother's last period"""
    return start_date + datetime.timedelta(weeks=40)


def possible_dates(
    due_date: datetime.datetime,
    buffer_before_due_date: int = 8 * 7,
    buffer_after_due_date: int = 4 * 7,
) -> pd.Series:
    """Gets all the reasonably possible dates (-8 weeks, + 4 weeks) of delivery"""
    idx = pd.date_range(
        due_date - datetime.timedelta(days=buffer_before_due_date),
        due_date + datetime.timedelta(days=buffer_after_due_date),
    )
    return pd.Series(data=idx, index=idx)
