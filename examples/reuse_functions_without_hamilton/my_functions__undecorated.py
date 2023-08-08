"""
This module defines some feature transformation logic that we'd like to apply.
It then in addition, decorates the functions with @check_output, so that at runtime, we can
execute a validator against the output of the function. The default is to log a warning, unless `importance='fail'`
is specified. In which case, it will halt execution.

Note:
    (1) to handle running on dask, ray, and spark, you will see some `@config.when*` decorators, which will
    determine whether this function should exist or not depending on some configuration passed in.
    (2) instead of @config.when* we could instead move these functions into specific independent modules, and then in
    the driver choose which one to use for the DAG. For the purposes of this example, we decided one file is simpler.
    (3) As of the 1.9.0 release, Hamilton, by default, does not handle data frame validation. Please use the Pandera
    integration - see `examples/data_quality/pandera` for an example.

"""
import numpy as np
import pandas as pd


def age_mean(age: pd.Series) -> np.float64:
    """Average of age"""
    return age.mean()


def age_zero_mean(age: pd.Series, age_mean: np.float64) -> pd.Series:
    """Zero mean of age"""
    return age - age_mean


def age_std_dev(age: pd.Series) -> np.float64:
    """Standard deviation of age."""
    return age.std()


def age_zero_mean_unit_variance(age_zero_mean: pd.Series, age_std_dev: np.float64) -> pd.Series:
    """Zero mean unit variance value of age"""
    return age_zero_mean / age_std_dev


def seasons_encoded(seasons: pd.Series) -> pd.DataFrame:
    """One hot encodes seasons into 4 dimensions:
    1 - first season
    2 - second season
    3 - third season
    4 - fourth season
    """
    return pd.get_dummies(seasons, prefix="seasons")


def seasons_extract__base(seasons: pd.Series) -> pd.DataFrame:
    """One hot encodes seasons into 4 dimensions:
    1 - first season
    2 - second season
    3 - third season
    4 - fourth season
    """
    return pd.get_dummies(seasons, prefix="seasons")


def extracted_seasons_1(seasons_extract: pd.DataFrame) -> pd.Series:
    """Returns column seasons_1"""
    return seasons_extract["seasons_1"]


def extracted_seasons_2(seasons_extract: pd.DataFrame) -> pd.Series:
    """Returns column seasons_2"""
    return seasons_extract["seasons_2"]


def extracted_seasons_3(seasons_extract: pd.DataFrame) -> pd.Series:
    """Returns column seasons_3"""
    return seasons_extract["seasons_3"]


def extracted_seasons_4(seasons_extract: pd.DataFrame) -> pd.Series:
    """Returns column seasons_4"""
    return seasons_extract["seasons_4"]


def day_of_week_encoded__base(day_of_the_week: pd.Series) -> pd.DataFrame:
    """One hot encodes day of week into five dimensions -- Saturday & Sunday weren't present.
    1 - Sunday, 2 - Monday, 3 - Tuesday, 4 - Wednesday, 5 - Thursday, 6 - Friday, 7 - Saturday.
    """
    return pd.get_dummies(day_of_the_week, prefix="day_of_the_week")


def day_of_the_week_2(day_of_week_encoded: pd.DataFrame) -> pd.Series:
    """Pulls out the day_of_the_week_2 column."""
    return day_of_week_encoded["day_of_the_week_2"]


def day_of_the_week_3(day_of_week_encoded: pd.DataFrame) -> pd.Series:
    """Pulls out the day_of_the_week_3 column."""
    return day_of_week_encoded["day_of_the_week_3"]


def day_of_the_week_4(day_of_week_encoded: pd.DataFrame) -> pd.Series:
    """Pulls out the day_of_the_week_4 column."""
    return day_of_week_encoded["day_of_the_week_4"]


def day_of_the_week_5(day_of_week_encoded: pd.DataFrame) -> pd.Series:
    """Pulls out the day_of_the_week_5 column."""
    return day_of_week_encoded["day_of_the_week_5"]


def day_of_the_week_6(day_of_week_encoded: pd.DataFrame) -> pd.Series:
    """Pulls out the day_of_the_week_6 column."""
    return day_of_week_encoded["day_of_the_week_6"]


def has_children(son: pd.Series) -> pd.Series:
    """Single variable that says whether someone has any children or not."""
    return (son > 0).astype(int)


def has_pet(pet: pd.Series) -> pd.Series:
    """Single variable that says whether someone has any pets or not."""
    return (pet > 0).astype(int)


def is_summer(month_of_absence: pd.Series) -> pd.Series:
    """Is it summer in Brazil? i.e. months of December, January, February."""
    return month_of_absence.isin([1, 2, 12]).astype(int)
