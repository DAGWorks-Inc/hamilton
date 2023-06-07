"""
My external decorator docstring. You can explain here what Hamilton is about!
"""

import numpy as np

from hamilton.function_modifiers import check_output, config, extract_columns


@check_output(range=(20.0, 60.0), data_type=np.float64)
def age_mean():
    pass


@check_output(range=(-120.0, 120.0), data_type=np.float64, allow_nans=False)
def age_zero_mean():
    pass


@check_output(range=(0.0, 40.0), data_type=np.float64)
def age_std_dev():
    pass


@check_output(range=(-4.0, 4.0), data_type=np.float64, allow_nans=False)
def age_zero_mean_unit_variance():
    pass


@extract_columns("seasons_1", "seasons_2", "seasons_3", "seasons_4")
def seasons_encodeds():
    pass


@config.when_not_in(execution=["spark", "dask"])
def seasons_encoded__base():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def seasons_1():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def seasons_2():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def seasons_3():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def seasons_4():
    pass


@config.when_not_in(execution=["spark", "dask"])
def day_of_week_encoded__base():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_2():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_3():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_4():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_5():
    pass


@check_output(data_type=np.uint8, values_in=[0, 1], allow_nans=False)
def day_of_the_week_6():
    pass


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def has_children():
    pass


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def has_pet():
    pass


@check_output(data_type=np.int64, values_in=[0, 1], allow_nans=False)
def is_summer():
    pass
