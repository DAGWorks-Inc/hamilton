import ibis
import ibis.expr.types as ir
import ibisml
import pandas as pd
from sklearn.base import BaseEstimator, clone
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold

from hamilton.function_modifiers import config, extract_fields
from hamilton.htypes import Collect, Parallelizable


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
    ids = feature_set.id.to_pandas()
    folds = KFold(n_splits=n_splits)
    for train_idx, val_idx in folds.split(ids):
        yield train_idx, val_idx


@extract_fields(
    dict(
        X_train=pd.DataFrame,
        X_val=pd.DataFrame,
        y_train=pd.DataFrame,
        y_val=pd.DataFrame,
    )
)
def prepare_data(
    feature_set: ir.Table,
    label: str,
    data_split: tuple,
    preprocessing_recipe: ibisml.Recipe,
) -> dict:
    train_idx, val_idx = data_split

    train_set = feature_set  # .filter(ibis._.id.isin(train_idx))
    val_set = feature_set  # .filter(ibis._.id.isin(val_idx))

    transform = preprocessing_recipe.fit(train_set, outcomes=[label])

    train = transform(train_set)
    df_train = train.to_pandas()
    X_train = df_train[train.features]
    y_train = df_train[train.outcomes]

    df_test = transform(val_set).to_pandas()
    X_val = df_test[train.features]
    y_val = df_test[train.outcomes]

    return dict(
        X_train=X_train,
        y_train=y_train,
        X_val=X_val,
        y_val=y_val,
    )


def cross_validation_fold(
    X_train: pd.DataFrame,
    X_val: pd.DataFrame,
    y_train: pd.DataFrame,
    y_val: pd.DataFrame,
    base_model: BaseEstimator,
    data_split: tuple,
) -> dict:
    train_idx, val_idx = data_split
    model = clone(base_model)

    model.fit(X_train, y_train)

    y_val_pred = model.predict(X_val)
    score = mean_squared_error(y_val, y_val_pred)

    return dict(id=val_idx, y_true=y_val, y_pred=y_val_pred, score=score)


@extract_fields(
    dict(
        cross_validation_scores=list[float],
        cross_validation_preds=list[dict],
    )
)
def cross_validation_fold_collection(cross_validation_fold: Collect[dict]) -> dict:
    scores, preds = [], []
    for fold in cross_validation_fold:
        scores.append(fold.pop("score"))
        preds.append(fold)
    return dict(
        cross_validation_scores=scores,
        cross_validation_preds=preds,
    )


def prediction_table(cross_validation_preds: list[dict]) -> ir.Table:
    return ibis.memtable(cross_validation_preds)


def store_predictions(prediction_table: ir.Table) -> bool:
    return True


@extract_fields(
    dict(
        full_model=BaseEstimator,
        fitted_recipe=ibisml.RecipeTransform,
    )
)
def train_full_model(
    feature_set: ir.Table,
    label: str,
    preprocessing_recipe: ibisml.Recipe,
    base_model: BaseEstimator,
) -> dict:
    transform = preprocessing_recipe.fit(feature_set, outcomes=[label])

    data = transform(feature_set)
    df = data.to_pandas()
    X = df[data.features]
    y = df[data.outcomes]

    base_model.fit(X, y)
    return dict(
        full_model=base_model,
        fitted_recipe=transform,
    )


if __name__ == "__main__":
    import model_training
    import table_dataflow

    from hamilton import driver
    from hamilton.execution.executors import SynchronousLocalTaskExecutor

    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(model_training, table_dataflow)
        .with_remote_executor(SynchronousLocalTaskExecutor())
        .with_config(dict(model="linear"))
        .build()
    )

    inputs = dict(
        raw_data_path="../data_quality/simple/Absenteeism_at_work.csv",
        feature_selection=[
            "id",
            "has_children",
            "has_pet",
            "is_summer_brazil",
            "service_time",
            "seasons",
            "disciplinary_failure",
            "absenteeism_time_in_hours",
        ],
        condition=ibis.ifelse(ibis._.has_pet == 1, True, False),
        label="absenteeism_time_in_hours",
    )
    dr.visualize_execution(
        final_vars=[
            "cross_validation_scores",
            "cross_validation_preds",
            "full_model",
            "fitted_recipe",
        ],
        output_file_path="cross_validation.png",
        inputs=inputs,
    )
    final_vars = ["cross_validation_scores"]

    res = dr.execute(final_vars, inputs=inputs)

    # df = res["feature_set"].to_pandas()
    print(res["cross_validation_scores"])  # .to_pandas())
    breakpoint()
    print()
