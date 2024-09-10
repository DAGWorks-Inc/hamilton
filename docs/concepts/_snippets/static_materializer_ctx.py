import pandas as pd
import xgboost


def preprocessed_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """preprocess raw data"""
    return ...


def model(preprocessed_df: pd.DataFrame) -> xgboost.XGBModel:
    """Train model on preprocessed data"""
    return ...


if __name__ == "__main__":
    import __main__

    from hamilton import driver
    from hamilton.io.materialization import from_, to

    data_path = "..."
    model_dir = "..."
    materializers = [
        from_.parquet(target="raw_df", path=data_path),
        to.json(
            id="model__json",  # name of the DataSaver node
            dependencies=["model"],
            path=f"{model_dir}/model.json",
        ),
    ]
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_materializers(*materializers)
        .build()
    )

    results = dr.execute(["model", "model__json"])
    # results["model"]  <- the model
    # results["model__json"] <- metadata from saving the model
