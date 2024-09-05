import pickle
from typing import Tuple

import pandas as pd

from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils

"""
TODO:
 - create a pipeline that will be used to show open lineage integration
 - one function loads from file
 - another loads from a database
 - one save to a file
 - another saves to a database
 - there are transform functions in between
"""


@dataloader()
def file_dataset(file_ds_path: str) -> Tuple[pd.DataFrame, dict]:
    # TODO: metadata
    df = pd.read_csv(file_ds_path)
    return df, utils.get_file_metadata(file_ds_path)


@dataloader()
def db_dataset(db_client: object) -> Tuple[pd.DataFrame, dict]:
    query = "SELECT * FROM purchase_data"
    return pd.read_sql(query, con=db_client), {
        "sql_metadata": {"query": query, "table_name": "purchase_data", "database": "sqlite"}
    }


def transformed_file_dataset(file_dataset: pd.DataFrame) -> pd.DataFrame:
    return file_dataset


def transformed_db_dataset(db_dataset: pd.DataFrame) -> pd.DataFrame:
    return db_dataset


def joined_dataset(
    transformed_file_dataset: pd.DataFrame, transformed_db_dataset: pd.DataFrame
) -> pd.DataFrame:
    return pd.concat([transformed_file_dataset, transformed_db_dataset], axis=1)


class ModelObject:
    def __init__(self):
        pass

    def predict(self, data):
        return data + 1


def fit_model(joined_dataset: pd.DataFrame) -> ModelObject:
    # model = ...
    return ModelObject()


@datasaver()
def saved_file(fit_model: ModelObject, file_path: str) -> dict:
    with open(file_path, "wb") as f:
        pickle.dump(fit_model, f)
    return utils.get_file_metadata(file_path)


@datasaver()
def saved_to_db(joined_dataset: pd.DataFrame, db_client: object, joined_table_name: str) -> dict:
    # joined_dataset.to_sql(joined_table_name, con=db_client, index=False)
    return utils.get_sql_metadata(joined_table_name, joined_dataset)


if __name__ == "__main__":
    import sqlite3

    from adapter import OpenLineageAdapter
    from openlineage.client import OpenLineageClient
    from openlineage.client.transport.file import FileConfig, FileTransport

    import __main__ as pipeline
    from hamilton import driver

    file_config = FileConfig(
        log_file_path="pipeline.json",
        append=True,
    )

    client = OpenLineageClient(transport=FileTransport(file_config))

    ola = OpenLineageAdapter(client, "my_namespace", "test_job")

    db_client = sqlite3.connect("purchase_data.db")

    dr = driver.Builder().with_modules(pipeline).with_adapters(ola).build()
    dr.display_all_functions("graph.png")
    result = dr.execute(
        ["saved_file", "saved_to_db"],
        inputs={
            "db_client": db_client,
            "file_ds_path": "data.csv",
            "file_path": "model.pkl",
            "joined_table_name": "joined_data",
        },
    )

    db_client.close()
