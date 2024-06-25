import pandas as pd
from sklearn import datasets

from hamilton.htypes import DataLoaderMetadata, DataSaverMetadata


def raw_data() -> tuple[pd.DataFrame, DataLoaderMetadata]:
    data = datasets.load_digits()
    df = pd.DataFrame(data.data, columns=[f"feature_{i}" for i in range(data.data.shape[1])])
    return df, DataLoaderMetadata.from_dataframe(df)


def transformed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data


def saved_data(transformed_data: pd.DataFrame, filepath: str) -> DataSaverMetadata:
    transformed_data.to_csv(filepath)
    return DataSaverMetadata.from_file_and_dataframe(filepath, transformed_data)


if __name__ == "__main__":
    import __main__ as simple_etl
    from hamilton_sdk import adapters

    from hamilton import driver

    tracker = adapters.HamiltonTracker(
        project_id=7,  # modify this as needed
        username="elijah@dagworks.io",
        dag_name="my_version_of_the_dag",
        tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
    )
    dr = driver.Builder().with_config({}).with_modules(simple_etl).with_adapters(tracker).build()
    dr.display_all_functions("simple_etl.png")

    dr.execute(["saved_data"], inputs={"filepath": "data.csv"})
