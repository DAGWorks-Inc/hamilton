import pandas as pd
import xgboost

from hamilton.function_modifiers import load_from, save_to, source


# source("data_path") allows to read the input value for `data_path`
@load_from.parquet(path=source("data_path"))
def preprocessed_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """preprocess raw data"""
    return ...


@save_to.json(path=source("model_path"))
def model(preprocessed_df: pd.DataFrame) -> xgboost.XGBModel:
    """Train model on preprocessed data"""
    return ...


if __name__ == "__main__":
    import __main__

    from hamilton import driver

    dr = driver.Builder().with_modules(__main__).build()

    data_path = "..."
    model_path = "..."
    inputs = dict(data_path=data_path, model_path=model_path)
    final_vars = ["save.model", "model"]
    results = dr.execute(final_vars, inputs=inputs)
    # results["model"]  <- the model
    # results["save.model"] <- metadata from saving the model
