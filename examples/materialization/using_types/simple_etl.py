from hamilton.telemetry import disable_telemetry

disable_telemetry()
import logging

import pandas as pd
from sklearn import datasets

from hamilton import node
from hamilton.function_modifiers import loader, saver
from hamilton.io import utils as io_utils
from hamilton.log_setup import setup_logging

setup_logging(logging.INFO)


@loader()
def raw_data() -> tuple[pd.DataFrame, dict]:
    data = datasets.load_digits()
    df = pd.DataFrame(data.data, columns=[f"feature_{i}" for i in range(data.data.shape[1])])
    metadata = io_utils.get_dataframe_metadata(df)
    return df, metadata


def transformed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data


@saver()
def saved_data(transformed_data: pd.DataFrame, filepath: str) -> dict:
    transformed_data.to_csv(filepath)
    metadata = io_utils.get_file_and_dataframe_metadata(filepath, transformed_data)
    return metadata


if __name__ == "__main__":
    import time

    from hamilton_sdk.tracking import runs

    df, metadata = raw_data()
    t1 = time.time()
    stats = runs.process_result(df, node.Node.from_fn(raw_data))
    t2 = time.time()
    print(t2 - t1)
