"""
This is a module that contains our "model fitting and related" transforms.
"""

import pandas as pd
from sklearn import base, linear_model


def model_classifier(random_state: int) -> base.ClassifierMixin:
    """Creates an unfitted LR model object.

    :param random_state:
    :return:
    """
    lr = linear_model.LogisticRegression(random_state=random_state)
    return lr


def trained_model(
    model_classifier: base.ClassifierMixin, train_set: pd.DataFrame, target_column_name: str
) -> base.ClassifierMixin:
    """Fits a new model.

    :param model_classifier:
    :param train_set:
    :return:
    """
    feature_cols = [c for c in train_set.columns if c != target_column_name]
    model_classifier.fit(train_set[feature_cols], train_set[target_column_name])
    return model_classifier
