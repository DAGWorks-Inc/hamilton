import pandas as pd

from hamilton.function_modifiers import extract_columns, tag


@extract_columns("date")
def sales_data_set_output_idx(sales_data_set: pd.DataFrame, output_idx: pd.Index) -> pd.DataFrame:
    return sales_data_set.loc[output_idx]


@tag(stage="production")
def month(date: pd.Series) -> pd.Series:
    return date.dt.month


@tag(stage="production")
def year(date: pd.Series) -> pd.Series:
    return date.dt.year


@tag(stage="production")
def week_of_year(date: pd.Series) -> pd.Series:
    x = date.dt.isocalendar().week
    x = x.astype("int64")
    return x


@tag(stage="production")
def day_of_week(date: pd.Series) -> pd.Series:
    return date.dt.dayofweek


@tag(stage="production")
def year_progress_pct(date: pd.Series) -> pd.Series:
    return date.dt.dayofyear / 365.25


@tag(stage="production")
def month_progress_pct(date: pd.Series) -> pd.Series:
    return date.dt.day / date.dt.days_in_month
