import ibis
import ibis.expr.types as ir 
import ibisml
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from sklearn.base import BaseEstimator, clone
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold

from hamilton.htypes import Parallelizable, Collect
from hamilton.function_modifiers import config, extract_fields


@config.when(model="linear")
def base_model__linear() -> BaseEstimator:
    return LinearRegression()


@config.when(model="random_forest")
def base_model__random_forest() -> BaseEstimator:
    return RandomForestRegressor()


@config.when(model="boosting")
def base_model__boosting() -> BaseEstimator:
    return HistGradientBoostingRegressor()


def preprocessing_recipe() -> ibisml.Recipe:
    return ibisml.Recipe(
        ibisml.Drop(["id"]),
        ibisml.ImputeMean(ibisml.numeric()),
        ibisml.ScaleStandard(ibisml.numeric()),
        ibisml.OneHotEncode(ibisml.nominal()),
    )
    

def data_split(
    feature_set: ir.Table,
    n_splits: int = 3,
) -> Parallelizable[tuple]:
    ids = feature_set.rowid().to_pandas()
    folds = KFold(n_splits=n_splits)
    for train_idx, val_idx in folds.split(ids):
        yield train_idx.tolist(), val_idx.tolist()


@extract_fields(dict(
    X_train=np.ndarray,
    X_val=np.ndarray,
    y_train=np.ndarray,
    y_val=np.ndarray,
    fitted_recipe=ibisml.RecipeTransform,
))
def prepare_data(
    feature_set: ir.Table,
    label: str,
    data_split: tuple,
    preprocessing_recipe: ibisml.Recipe,
) -> dict:
    train_idx, val_idx = data_split
    
    train = feature_set.filter(ibis._.rowid().isin(train_idx))
    test = feature_set.filter(ibis._.rowid().isin(val_idx))
    
    transform = preprocessing_recipe.fit(train, outcomes=[label])
    
    df_train = transform(train).to_pandas()
    X_train = df_train[transform.features]
    y_train = df_train[transform.outcomes]
    
    df_test = transform(test).to_pandas()
    X_test = df_test[transform.features]
    y_test = df_test[transform.outcomes]
    
    return dict(
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        fitted_recipe=transform,
    )

  
def cross_validation_fold(
    X_train: np.ndarray,
    X_val: np.ndarray,
    y_train: np.ndarray,
    y_val: np.ndarray,
    base_model: BaseEstimator,
    data_split: tuple,
) -> dict: 
    train_idx, val_idx = data_split
    model = clone(base_model)

    model.fit(X_train, y_train)
    
    y_val_pred = model.predict(X_val)
    metric = mean_squared_error(y_val, y_val_pred)

    return dict(
        val_idx=val_idx,
        y_val_pred=y_val_pred,
        metric=metric
    )


def cross_validation_fold_collection(
    cross_validation_fold: Collect[dict]
) -> list[dict]:
    return list(cross_validation_fold)


def prediction_df(cross_validation_fold_collection: list) -> pd.DataFrame:
    return #pd.DataFrame.from_dict(dict(y_true=y, y_pred=y_pred), orient="columns")


def store_predictions(prediction_df: pd.DataFrame) -> bool:
    return True


def full_model(
    feature_set: ir.Table,
    label: str,
    preprocessing_recipe: ibisml.Recipe,
    base_model: BaseEstimator,
) -> BaseEstimator:
    transform = preprocessing_recipe.fit(feature_set, outcomes=[label])
    
    df = transform(feature_set).to_pandas()
    X = df[transform.features]
    y = df[transform.outcomes]
    
    base_model.fit(X, y)
    return base_model
