import abc
import os
from types import ModuleType
from typing import Any, Dict, List, Optional

import click
import duckdb as duckdb
import pandas as pd

from examples.data_loaders.utils import generate_random_walk_time_series
from hamilton import ad_hoc_utils, driver, function_modifiers
from hamilton.function_modifiers import tag

"""Simple file to generate test data. This will then be saved to various locations for the rest of the example."""


def index(start_date: str = "20200101", end_date: str = "20220901") -> pd.Series:
    return pd.Series(pd.date_range(start_date, end_date))


@tag(**{"materialization.table": "marketing_spend"})
def marketing_spend_by_channel(index: pd.Series) -> pd.DataFrame:
    """Marketing spend by channel. Randomly generated, meant to be increasing to broadcast growth.
    In this simple case, marketing spend is not partitioned by business line

    :param index: TS index to use to generate data
    :return:
    """
    data = {
        "facebook": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=10000,
            step_mean=100,
            step_stddev=2000,
            min_value=0,
        ),
        "twitter": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=10000,
            step_mean=50,
            step_stddev=1000,
            min_value=0,
        ),
        "tv": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=15000,
            step_mean=40,
            step_stddev=1400,
            min_value=0,
        ),
        "youtube": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=10000,
            step_mean=40,
            step_stddev=1600,
            min_value=0,
        ),
        "radio": generate_random_walk_time_series(
            num_datapoints=len(index), start_value=5000, step_mean=20, step_stddev=800, min_value=0
        ),
        "billboards": generate_random_walk_time_series(
            num_datapoints=len(index), start_value=1000, step_mean=10, step_stddev=800, min_value=0
        ),
        "date": index,
    }
    return pd.DataFrame(data=data)


@tag(**{"materialization.table": "signups"})
def signups_by_business_line(index: pd.Series) -> pd.DataFrame:
    data = {
        "womens": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=1000,
            step_mean=1,
            step_stddev=20,
            min_value=0,
            apply=int,
        ),
        "mens": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=1000,
            step_mean=1,
            step_stddev=20,
            min_value=0,
            apply=int,
        ),
        "date": index,
    }
    return pd.DataFrame(data)


@tag(**{"materialization.table": "churn"})
def churn_by_business_line(index: pd.Series) -> pd.DataFrame:
    data = {
        "womens": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=100,
            step_mean=0.05,
            step_stddev=3,
            min_value=0,
            apply=int,
        ),
        "mens": generate_random_walk_time_series(
            num_datapoints=len(index),
            start_value=100,
            step_mean=0.05,
            step_stddev=3,
            min_value=0,
            apply=int,
        ),
        "date": index,
    }
    return pd.DataFrame(data)


@click.group()
def main():
    pass


class MaterializationDriver(driver.Driver, abc.ABC):
    def __init__(self, config: Dict[str, Any], *modules: ModuleType):
        super(MaterializationDriver, self).__init__(config, *modules)

    @abc.abstractmethod
    def materialize(self, df: pd.DataFrame, table: str):
        """Materializes (saves) the specified dataframe to a db/table combo

        :param db:
        :param table:
        :return:
        """
        pass

    def materialize_to(self, var: driver.Variable) -> Optional[str]:
        """Returns a db, dtable tuple of materialization

        :param var: Variable representing the node in the hamilton DAG
        :return: None if we want to bypass materialization, else a string representing the "table"
        """
        if "materialization.table" in var.tags:
            if var.type != pd.DataFrame:
                raise ValueError(
                    f"Node: {var.name} requests materialization but does not produce a pandas dataframe, rather a: {var.type}"
                )
            return var.tags["materialization.table"]
        return None

    def execute_and_materialize(
        self, overrides: Dict[str, Any] = None, inputs: Dict[str, Any] = None
    ):
        """Executes and materializes it

        :param overrides:
        :param inputs:
        :return:
        """
        nodes_to_materialize = [
            var for var in self.list_available_variables() if self.materialize_to(var) is not None
        ]
        raw_execute_results = self.raw_execute(
            [var.name for var in nodes_to_materialize], overrides=overrides, inputs=inputs
        )
        for node in nodes_to_materialize:
            self.materialize(raw_execute_results[node.name], self.materialize_to(node))


class DuckDBMaterializationDriver(MaterializationDriver):
    def __init__(self, path: str, config: Dict[str, Any], modules: List[ModuleType]):
        super(DuckDBMaterializationDriver, self).__init__(config, *modules)
        self.con = duckdb.connect(database=path, read_only=False)

    def materialize(self, df: pd.DataFrame, table: str):
        self.con.execute(f"CREATE TABLE {table} AS SELECT * from df")
        self.con.fetchall()

    def close(self):
        self.con.close()


class CSVMaterializationDriver(MaterializationDriver):
    def __init__(self, path: str, config: Dict[str, Any], modules: List[ModuleType]):
        super(CSVMaterializationDriver, self).__init__(config, *modules)
        self.path = path

    def materialize(self, df: pd.DataFrame, table: str):
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)
        df.to_csv(os.path.join(self.path, f"{table}.csv"))


def _get_module() -> ModuleType:
    return ad_hoc_utils.create_temporary_module(
        index, marketing_spend_by_channel, signups_by_business_line, churn_by_business_line
    )


@main.command()
@click.option("--db-path", type=click.Path(exists=False), required=True)
def setup_duck_db(db_path: str):
    driver = DuckDBMaterializationDriver(path=db_path, config={}, modules=[_get_module()])
    driver.execute_and_materialize()
    driver.close()


@main.command()
@click.option("--db-path", type=click.Path(exists=False))
def setup_csv(db_path: str):
    driver = CSVMaterializationDriver(path=db_path, config={}, modules=[_get_module()])
    driver.execute_and_materialize()


if __name__ == "__main__":
    main()
