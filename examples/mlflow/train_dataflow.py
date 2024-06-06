from typing import Dict, Union

import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.datasets import fetch_openml
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

from hamilton.function_modifiers import extract_fields


@extract_fields(
    {"X_train": pd.DataFrame, "X_test": pd.DataFrame, "y_train": pd.Series, "y_test": pd.Series}
)
def dataset_splits() -> Dict[str, Union[pd.DataFrame, pd.Series]]:
    """Load the titanic dataset and partition it in X_train, y_train, X_test, y_test"""
    X, y = fetch_openml("titanic", version=1, as_frame=True, return_X_y=True)

    feature_cols = ["fare", "age"]
    X = X[feature_cols].fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(X[feature_cols], y)
    return {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}


def trained_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> BaseEstimator:
    """Fit a binary classifier on the training data"""
    model = LogisticRegression()
    model.fit(X_train, y_train)
    return model


if __name__ == "__main__":
    import __main__

    from hamilton import driver
    from hamilton.io.materialization import to

    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_materializers(
            to.mlflow(
                id="trained_model__mlflow",
                dependencies=["trained_model"],
            ),
        )
        .build()
    )

    results = dr.execute(["trained_model__mlflow"])
    print(results)
