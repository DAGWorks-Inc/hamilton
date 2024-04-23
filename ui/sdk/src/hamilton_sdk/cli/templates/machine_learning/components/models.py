"""This module contains specific incarnations of models."""

from sklearn import base

from hamilton.function_modifiers import source, subdag

try:
    import model_fitting
except ImportError:
    from . import model_fitting


@subdag(
    model_fitting,
    inputs={
        "data_set": source("data_set_v1"),
    },
    config={"clf": "svm", "shuffle_train_test_split": True, "test_size_fraction": 0.2},
)
def svm_model(
    fit_clf: base.ClassifierMixin, training_accuracy: float, testing_accuracy: float
) -> dict:
    return {
        "svm": fit_clf,
        "training_accuracy": training_accuracy,
        "testing_accuracy": testing_accuracy,
    }


@subdag(
    model_fitting,
    inputs={
        "data_set": source("data_set_v1"),
    },
    config={
        "clf": "logistic",
        "shuffle_train_test_split": True,
        "test_size_fraction": 0.2,
        "penalty": "l2",
    },
)
def lr_model(
    fit_clf: base.ClassifierMixin, training_accuracy: float, testing_accuracy: float
) -> dict:
    return {
        "logistic": fit_clf,
        "training_accuracy": training_accuracy,
        "testing_accuracy": testing_accuracy,
    }


def best_model(svm_model: dict, lr_model: dict) -> dict:
    """Returns the best model based on the testing accuracy."""
    if svm_model["testing_accuracy"] > lr_model["testing_accuracy"]:
        return svm_model
    else:
        return lr_model
