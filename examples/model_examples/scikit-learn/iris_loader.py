import numpy as np
from sklearn import datasets, utils

"""
Module to load iris data.
"""


def iris_data() -> utils.Bunch:
    return datasets.load_iris()


def target(iris_data: utils.Bunch) -> np.ndarray:
    return iris_data.target


def target_names(iris_data: utils.Bunch) -> np.ndarray:
    return iris_data.target_names


def feature_matrix(iris_data: utils.Bunch) -> np.ndarray:
    return iris_data.data
