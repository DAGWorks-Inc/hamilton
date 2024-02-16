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

    # this registers DataSaver and DataLoader objects
    from hamilton.plugins import pandas_extensions, xgboost_extensions

    dr = driver.Builder().with_modules(__main__).build()

    data_path = "..."
    model_dir = "..."
    materializers = [
        from_.parquet(path=data_path, target="raw_df"),
        to.json(path=f"{model_dir}/model.json", dependencies=["model"], id="model__json"),
    ]

    dr.materialize(*materializers)
