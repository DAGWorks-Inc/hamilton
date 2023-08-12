import numpy as np
from sklearn import datasets, utils

from hamilton.function_modifiers import config

"""
Module to load digit data.
"""


@config.when(data_loader="iris")
def data__iris() -> utils.Bunch:
    return datasets.load_digits()


@config.when(data_loader="digits")
def data__digits() -> utils.Bunch:
    return datasets.load_digits()


def target(data: utils.Bunch) -> np.ndarray:
    return data.target


def target_names(data: utils.Bunch) -> np.ndarray:
    return data.target_names


def feature_matrix(data: utils.Bunch) -> np.ndarray:
    return data.data
