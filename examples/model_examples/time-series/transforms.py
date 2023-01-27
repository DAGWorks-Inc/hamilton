from typing import Optional, Tuple

import pandas as pd
from pandas.core.groupby import generic
from sklearn import preprocessing

from hamilton.function_modifiers import parameterize, source, value


def _label_encoder(col: pd.Series) -> Tuple[preprocessing.LabelEncoder, pd.Series]:
    """Creates an encoder, fits itself on the input, and then transforms the input.

    :param col: the column to encode.
    :return: tuple of fit encoder, and the encoded column.
    """
    encoder = preprocessing.LabelEncoder()
    encoder = encoder.fit(col)
    return encoder, encoder.transform(col)


# this doesn't seem useful at all -- so skipping it
# encoder = preprocessing.LabelEncoder()
# data['id_encode'] = encoder.fit_transform(data['id'])
cols_to_encode = [
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
]
mapping = {f"{name}_encoded": dict(column_to_encode=source(name)) for name in cols_to_encode}


@parameterize(**mapping)
def label_encoder(column_to_encode: pd.Series) -> pd.Series:
    """Creates a categorically encoded column from {column_to_encode}.

    :param column_to_encode:
    :return: encoded column
    """
    _, encoded = _label_encoder(column_to_encode)
    return encoded


def grouped_demand(demand: pd.Series) -> generic.SeriesGroupBy:
    """Groups the demands column by id (i.e. the index).

    :param demand:
    :return:
    """
    return demand.groupby(level=0)


@parameterize(
    lag_t28={"lag": value(28)},
    lag_t29={"lag": value(29)},
    lag_t30={"lag": value(30)},
    rolling_mean_t7={"lag": value(28), "window": value(7), "transform": value("mean")},
    rolling_std_t7={"lag": value(28), "window": value(7), "transform": value("std")},
    rolling_mean_t30={"lag": value(28), "window": value(30), "transform": value("mean")},
    rolling_mean_t90={"lag": value(28), "window": value(90), "transform": value("mean")},
    rolling_mean_t180={"lag": value(28), "window": value(180), "transform": value("mean")},
    rolling_std_t30={"lag": value(28), "window": value(30), "transform": value("std")},
)
def transform_grouped_demand(
    grouped_demand: generic.SeriesGroupBy,
    lag: int,
    window: Optional[int] = None,
    transform: Optional[str] = None,
) -> pd.Series:
    """Transforms the grouped demand by lagging, applying a rolling mean,
    then shifting by a specified amount.

    :param grouped_demand: Grouped series of demand data
    :param shift_by: How much to lag
    :param window: The window to apply the rolling transformation over (if applicable)
    :param transform: The transformation to apply (if applicable)
    :return: Transformed demand data
    """
    # First shift
    out = grouped_demand.transform(lambda x: x.shift(lag))
    if window is not None:
        out = out.rolling(window)
        out = getattr(out, transform)()
    return out


def grouped_sell_price(sell_price: pd.Series) -> generic.SeriesGroupBy:
    return sell_price.groupby(level=0)


# price features
def lag_price_t2(grouped_sell_price: generic.SeriesGroupBy) -> pd.Series:
    return grouped_sell_price.transform(lambda x: x.shift(2))


def price_change_t2(lag_price_t2: pd.Series, sell_price: pd.Series) -> pd.Series:
    return (lag_price_t2 - sell_price) / lag_price_t2


def rolling_price_max_t365(grouped_sell_price: generic.SeriesGroupBy) -> pd.Series:
    return grouped_sell_price.transform(lambda x: x.shift(1).rolling(365).max())


def rolling_price_max_t730(grouped_sell_price: generic.SeriesGroupBy) -> pd.Series:
    return grouped_sell_price.transform(lambda x: x.shift(1).rolling(730).max())


def price_change_t365(rolling_price_max_t365: pd.Series, sell_price: pd.Series) -> pd.Series:
    return (rolling_price_max_t365 - sell_price) / rolling_price_max_t365


def price_change_t730(rolling_price_max_t730: pd.Series, sell_price: pd.Series) -> pd.Series:
    return (rolling_price_max_t730 - sell_price) / rolling_price_max_t730


def rolling_price_std_t7(grouped_sell_price: generic.SeriesGroupBy) -> pd.Series:
    return grouped_sell_price.transform(lambda x: x.rolling(7).std())


def rolling_price_std_t30(grouped_sell_price: generic.SeriesGroupBy) -> pd.Series:
    return grouped_sell_price.transform(lambda x: x.rolling(30).std())


# time features
def year(date: pd.Series) -> pd.Series:
    return date.dt.year


def month(date: pd.Series) -> pd.Series:
    return date.dt.month


def week(date: pd.Series) -> pd.Series:
    wk = date.dt.isocalendar().week.astype(int)  # need to cast because lightgbm complains
    return wk


def day(date: pd.Series) -> pd.Series:
    return date.dt.day


def dayofweek(date: pd.Series) -> pd.Series:
    return date.dt.dayofweek
