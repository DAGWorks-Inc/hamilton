import pandas as pd
from sklearn import datasets

from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils as io_utils


@dataloader()
def raw_data() -> tuple[pd.DataFrame, dict]:
    data = datasets.load_digits()
    df = pd.DataFrame(data.data, columns=[f"feature_{i}" for i in range(data.data.shape[1])])
    metadata = io_utils.get_dataframe_metadata(df)
    return df, metadata


def transformed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data


@datasaver()
def saved_data(transformed_data: pd.DataFrame, filepath: str) -> dict:
    transformed_data.to_csv(filepath)
    metadata = io_utils.get_file_and_dataframe_metadata(filepath, transformed_data)
    return metadata
