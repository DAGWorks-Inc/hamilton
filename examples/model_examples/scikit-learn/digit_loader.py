import numpy as np
from sklearn import datasets, utils

"""
Module to load digit data.
"""


def digit_data() -> utils.Bunch:
    return datasets.load_digits()


def target(digit_data: utils.Bunch) -> np.ndarray:
    return digit_data.target


def target_names(digit_data: utils.Bunch) -> np.ndarray:
    return digit_data.target_names


def feature_matrix(digit_data: utils.Bunch) -> np.ndarray:
    # return digit_data.images.reshape((len(digit_data), -1))
    return digit_data.data
