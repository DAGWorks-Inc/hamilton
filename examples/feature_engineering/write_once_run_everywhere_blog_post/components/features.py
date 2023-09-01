from datetime import datetime

import pandas as pd


def is_male(gender: pd.Series) -> pd.Series:
    return gender == "male"


def is_female(gender: pd.Series) -> pd.Series:
    return gender == "female"


def is_high_roller(budget: pd.Series) -> pd.Series:
    return budget > 100


def age_normalized(age: pd.Series, age_mean: float, age_stddev: float) -> pd.Series:
    return (age - age_mean) / age_stddev


def time_since_last_login(execution_time: datetime, last_logged_in: pd.Series) -> pd.Series:
    return execution_time - last_logged_in
