from typing import Dict

import numpy as np
import pandas as pd
from sklearn import datasets, svm, metrics
from sklearn.model_selection import train_test_split

from hamilton import function_modifiers


def flattened_digits(digit_data: np.ndarray) -> np.ndarray:
    return digit_data.reshape((len(digit_data), -1))


@function_modifiers.config.when(clf='svm')
def prefit_clf__svm(gamma: float = 0.001) -> svm.SVC:
    return svm.SVC(gamma=gamma)


@function_modifiers.extract_columns(*['X_train', 'X_test', 'y_train', 'y_test'])
def train_test_split_func(flattened_digits: np.ndarray, digits_targets: np.ndarray, test_size_fraction: float = 0.5,
                          shuffle_train_test_split: bool = False) -> Dict[str, np.ndarray]:
    X_train, X_test, y_train, y_test = train_test_split(
        flattened_digits, digits_targets, test_size=test_size_fraction, shuffle=shuffle_train_test_split
    )
    return {
        'X_train': X_train, 'X_test': X_test, 'y_train': y_train, 'y_test': y_test
    }

def fit_clf(prefit_clf: svm.SVC, X_train: np.ndarray, y_train: np.ndarray) -> svm.SVC:
    """This mutates the prefit_clf"""
    prefit_clf.fit(X_train, y_train)
    return prefit_clf

def predict_output(fit_clf: svm.SVC, X_test: np.ndarray) -> np.ndarray:
    return fit_clf.predict(X_test)

def classification_report(predict_output: np.ndarray, y_test: np.ndarray) -> str:
    return metrics.classification_report(y_test, predict_output)

# def confusion_matrix(predict_output: np.ndarray, y_test: np.ndarray) -> str:
    # return metrics.ConfusionMatrixDisplay.from_predictions(y_test, predict_output)
