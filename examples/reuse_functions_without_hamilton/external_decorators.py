"""
My external decorator docstring. You can explain here what Hamilton is about!
"""
import numpy as np
import pandas as pd

from hamilton.function_modifiers import check_output, config, extract_columns


@check_output(range=(20.0, 60.0), data_type=np.float64)
def age_mean():
    ...


@check_output(range=(-120.0, 120.0), data_type=np.float64, allow_nans=False)
def age_zero_mean():
    ...


@check_output(range=(0.0, 40.0), data_type=np.float64)
def age_std_dev():
    ...


@check_output(range=(-4.0, 4.0), data_type=np.float64, allow_nans=False)
def age_zero_mean_unit_variance():
    ...


@extract_columns("seasons_1", "seasons_2", "seasons_3", "seasons_4")
def seasons_encoded() -> pd.DataFrame:
    ...


@config.when_not_in(execution=["spark", "dask"])
def seasons_extract__base():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def extracted_seasons_1():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def extracted_seasons_2():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def extracted_seasons_3():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def extracted_seasons_4():
    ...


@config.when_not_in(execution=["spark", "dask"])
def day_of_week_encoded__base():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_2():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_3():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_4():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_5():
    ...


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_6():
    ...


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def has_children():
    ...


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def has_pet():
    ...


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def is_summer():
    ...
