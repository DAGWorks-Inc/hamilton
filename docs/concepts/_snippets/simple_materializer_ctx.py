import pandas as pd
import xgboost

from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils


@dataloader()
def raw_df(data_path: str) -> tuple[pd.DataFrame, dict]:
    """Load raw data from parquet file"""
    df = pd.read_parquet(data_path)
    return df, utils.get_file_and_dataframe_metadata(data_path, df)


def preprocessed_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """preprocess raw data"""
    return ...


def model(preprocessed_df: pd.DataFrame) -> xgboost.XGBModel:
    """Train model on preprocessed data"""
    return ...


@datasaver()
def save_model(model: xgboost.XGBModel, model_dir: str) -> dict:
    """Save trained model to JSON format"""
    model.save_model(f"{model_dir}/model.json")
    return utils.get_file_metadata(f"{model_dir}/model.json")


if __name__ == "__main__":
    import __main__

    from hamilton import driver

    dr = driver.Builder().with_modules(__main__).build()
    data_path = "..."
    model_dir = "..."
    inputs = dict(data_path=data_path, model_dir=model_dir)
    final_vars = ["save_model"]
    results = dr.execute(final_vars, inputs=inputs)
    # results["save_model"] == None
