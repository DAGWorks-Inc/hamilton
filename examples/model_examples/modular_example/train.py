from typing import Any

import pandas as pd

from hamilton.function_modifiers import config


@config.when(model="RandomForest")
def base_model__rf(model_params: dict) -> Any:
    from sklearn.ensemble import RandomForestClassifier

    return RandomForestClassifier(**model_params)


@config.when(model="LogisticRegression")
def base_model__lr(model_params: dict) -> Any:
    from sklearn.linear_model import LogisticRegression

    return LogisticRegression(**model_params)


@config.when(model="XGBoost")
def base_model__xgb(model_params: dict) -> Any:
    from xgboost import XGBClassifier

    return XGBClassifier(**model_params)


def fit_model(transformed_data: pd.DataFrame, base_model: Any) -> Any:
    """Fit a model to transformed data."""
    base_model.fit(transformed_data.drop("target", axis=1), transformed_data["target"])
    return base_model
