import dataclasses
import os
from concurrent.futures import Future
from typing import Any, Collection, Dict, List, Optional, Tuple, Type

import dask
import pandas as pd
from polars import DataFrame

from examples.enterprise_ready.preamble import Model
from hamilton import driver, graph_types, registry
from hamilton.data_quality.base import ValidationResult
from hamilton.execution.executors import (
    ExecutionManager,
    SynchronousLocalTaskExecutor,
    TaskExecutor,
    TaskFuture,
    TaskFutureWrappingPythonFuture,
)
from hamilton.execution.grouping import TaskImplementation
from hamilton.function_modifiers import extract_fields, load_from, source, tag
from hamilton.io.data_adapters import DataSaver
from hamilton.io.materialization import from_, to
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook


def _derive_model_type(result):
    pass


class my_library:
    @staticmethod
    def query(**kwargs) -> pd.DataFrame:
        pass


class modal_or_ray_or_something_else:
    @staticmethod
    def submit(**kwargs) -> Future:
        pass


def _load_secrets(*args) -> dict:
    pass


class Connection:
    pass


def _create_snowflake_connection(**kwargs) -> Connection:
    pass


def _query_snowflake() -> pd.DataFrame:
    pass


def _load_data(param) -> pd.DataFrame:
    pass


def _clean_data(param) -> pd.DataFrame:
    pass


def _process_features(param) -> pd.DataFrame:
    pass


def _process_data(data) -> pd.DataFrame:
    pass


# custom_materialization 0
from hamilton.io import materialization


@dataclasses.dataclass
class WrapExistingIOLibrary(materialization.DataLoader):
    query: str  # SQL query
    database: str  #

    def load_data(self, type_: Type[Type]) -> Tuple[pd.DataFrame, dict]:
        df = my_library.query(db=self.database, query=self.query)
        return (df, {"query": self.query, "database": self.database, "num_rows": len(df)})

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [pd.DataFrame]

    @classmethod
    def name(cls) -> str:
        return "my_source_name"


registry.register_adapter(WrapExistingIOLibrary)

dr = driver.Builder().with_modules(...).build()
dr.materialize(
    from_.my_custom_loader(
        query="SELECT * FROM my_table",
        database="my_teams_research_data",
        target="input_name_to_load_to",
    ),
    to.somewhere(...),
)


# credentials 0


# secrets.py
@extract_fields({"snowflake_username": str, "snowflake_password": str, "snowflake_account": str})
def snowflake_credentials() -> dict:
    # delegate to a secrets manager
    return _load_secrets("snowflake_username", "snowflake_password", "snowflake_account")


# data.py
def connection(snowflake_credentials: dict, db: str) -> Connection:
    return _create_snowflake_connection(**snowflake_credentials, db=db)


# credentials 1
class CredentialsSetupHook(GraphExecutionHook):
    def run_before_graph_execution(self, **future_kwargs: Any):
        secrets = _load_secrets("snowflake_username", "snowflake_password", "snowflake_account")
        os.environ["SNOWFLAKE_USERNAME"] = secrets["snowflake_username"]
        os.environ["SNOWFLAKE_PASSWORD"] = secrets["snowflake_password"]
        os.environ["SNOWFLAKE_ACCOUNT"] = secrets["snowflake_account"]

    def run_after_graph_execution(self, **future_kwargs: Any):
        del os.environ["SNOWFLAKE_USERNAME"]
        del os.environ["SNOWFLAKE_PASSWORD"]
        del os.environ["SNOWFLAKE_ACCOUNT"]


# credentials 2


@dataclasses.dataclass
class SnowflakeLoader(materialization.DataLoader):
    query: str
    conn: Connection

    def load_data(self, type_: Type[Type]) -> Tuple[pd.DataFrame, dict]:
        for key in ["SNOWFLAKE_USERNAME", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT"]:
            assert key in os.environ, (
                f"Environment variable: {key} is not set. "
                f"Please ensure it is set by using the CredentialsSetupHook! "
                f"You can do this by..."
            )
        return _query_snowflake(...)

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [pd.DataFrame]

    @classmethod
    def name(cls) -> str:
        return "snowflake"


# execution_infra 0

# data_loading.py


@load_from.csv_s3(uri=source("training_data_uri"))
def cleaned_data(loaded_data: pd.DataFrame) -> pd.DataFrame:
    return _clean_data(data)


# data_processing.py


def filtered_data(data: DataFrame) -> DataFrame:
    return data.where(...)


def feature_data(filtered_data: DataFrame) -> DataFrame:
    return _process_features(filtered_data)


# run.py

production_driver = driver.Builder().with_modules(data_loading, data_processing).build()

features = production_driver.execute(["feature_data"], inputs={})

test_driver = driver.Builder().with_modules(data_processing).build()

results = test_driver.execute(["feature_data"], inputs={"loaded_data": _generate_test_data(...)})

# execution_infra 1


# data_loading.py
from hamilton.function_modifiers import config, load_from


@load_from.csv(path="local_data.csv")
@config.when(env="dev")
def cleaned_data__dev(loaded_df: pd.DataFrame) -> pd.DataFrame:
    # clean, test data in dev
    return loaded_df


@load_from.csv_s3(uri=source("training_data_uri"))
@config.when(env="prod")
def cleaned_data_prod(loaded_df: pd.DataFrame) -> pd.DataFrame:
    return _clean_data(loaded_df)


# scaling 0 Pyspark UDF graph adapter

import pandas as pd
import pyspark.sql as ps

from hamilton.plugins import h_dask, h_spark


def raw_data() -> ps.DataFrame:
    return ...


def feature_1(column_1_from_df: pd.Series) -> pd.Series:
    return ...


def feature_2(column_2_from_df: pd.Series) -> pd.Series:
    return ...


def feature_3(feature_1: pd.Series, feature_2: pd.Series) -> pd.Series:
    return ...


@h_spark.with_columns(
    feature_1, feature_2, feature_3, columns_to_pass=["column_1_from_df", "column_2_from_df"]
)
def df_with_features(raw_data: ps.DataFrame) -> ps.DataFrame:
    return raw_data


# scaling 1 -- delegate to an external executor


def requires_resources(gpu: int = None, cpu: int = None, memory_gb: int = None):
    return tag(requires_gpu=str(gpu), requires_cpu=str(cpu), requires_memory_gb=str(memory_gb))


# model_training.py


def training_data() -> pd.DataFrame:
    return _load_data(...)


@requires_resources(gpu=2, cpu=10)
def train_model(training_data: pd.DataFrame) -> Model:
    ...  # train your model with a big machine


class DelegatingExecutionManager(ExecutionManager):
    def __init__(self, remote_executor: TaskExecutor):
        self.local_executor = SynchronousLocalTaskExecutor()
        self.remote_executor = remote_executor
        super().__init__([self.local_executor, remote_executor])

    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        for node in task.nodes:
            if "required_cpu" in node.tags or "required_gpu" in node.tags:
                return self.remote_executor
        return self.local_executor


class RemoteExecutor(TaskExecutor):
    def submit_task(self, task: TaskImplementation) -> TaskFuture:
        required_cpu, required_gpu, required_memory_gb = 0, 0, 0
        for node in task.nodes:
            required_cpu = max(int(node.tags.get("required_cpu", "0")), required_cpu)
            required_gpu = max(int(node.tags.get("required_gpu", "0")), required_gpu)
        return TaskFutureWrappingPythonFuture(
            modal_or_ray_or_something_else.submit(cpu=required_cpu, gpu=required_gpu)
        )

    # a few methods left out for brevity


dr = (
    driver.Builder()
    .with_modules(model_training)
    .enable_dynamic_execution()
    .with_execution_manager(DelegatingExecutionManager(RemoteExecutor()))
    .build()
)

# scaling 2 dask/modin integration

from dask.distributed import Client, LocalCluster

cluster = LocalCluster()  # Replace with your cluster
client = Client(cluster)

graph_adapter = h_dask.DaskGraphAdapter(
    client,
    h_dask.DaskDataFrameResult(),
    use_delayed=False,
    compute_at_end=True,
)

dr = driver.Builder().with_modules(...).with_adapters(graph_adapter).build()


# Ensure that your data loaders return dask dataframes, and the rest should (largely) work

# track_experiments 0


@dataclasses.dataclass
class MLFLowSaver(DataSaver):
    """Our MLFlow Materializer"""

    experiment_name: str
    model_type: str  # e.g. "pyfunc", "sklearn", "spark", "onnx", "pytorch", "tensorflow", "xgboost"
    artifact_path: str
    run_name: str = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [object]

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    def save_data(self, model: object) -> Dict[str, Any]:
        mlflow.set_experiment(self.experiment_name)
        # Initiate the MLflow run context
        with mlflow.start_run(run_name=self.run_name) as run:
            ml_logger = getattr(mlflow, self.model_type)
            model_info = ml_logger.log_model(
                model,
                self.artifact_path,
            )
        return {
            "model_info": model_info.__dict__,  # return some metadata
            "run_info": run.to_dictionary(),
            "mlflow_uri": f"{mlflow.get_tracking_uri()}/#/experiments/{run.info.experiment_id}/runs/{run.info.run_id}",
        }


# track_experiments 1


def artifact(fn):
    return tag(properties=["artifact"])(fn)


class MLFlowSaverHook(NodeExecutionHook):
    def __init__(self, run_name: str, experiment_name: str):
        self.experiment_name = run_name
        self.run_name = run_name

    def run_before_node_execution(self, **future_kwargs: Any):
        pass

    def run_after_node_execution(
        self, *, node_name: str, node_tags: Dict[str, Any], result: Any, **future_kwargs: Any
    ):
        if result is not None and "artifact" in node_tags.get("properties", []):
            materializer = MLFLowSaver(
                experiment_name=self.experiment_name,
                model_type=_derive_model_type(result),
                artifact_path=node_name,
                run_name=self.run_name,
            )
            materializer.save_data(result)


@artifact
def model(training_data: DataFrame, hyperparameters: dict) -> Model:
    return Model(**hyperparameters).train(training_data)


# data_quality 0

import pandera as pa

from hamilton import function_modifiers


@function_modifiers.check_output(
    schema=pa.DataFrameSchema(
        {
            "a": pa.Column(pa.Int),
            "b": pa.Column(pa.Float),
            "c": pa.Column(pa.String, nullable=True),
            "d": pa.Column(pa.Float),
        }
    )
)
def dataset() -> pd.DataFrame:
    return ...


# data_quality 1

from hamilton.data_quality import base


class NoNullValidator(base.DataValidator):
    def applies_to(self, datatype: Type[Type]) -> bool:
        return issubclass(datatype, pd.Series)

    def description(self) -> str:
        return "Ensures that the data has no nulls"

    @classmethod
    def name(cls) -> str:
        return "no_nulls"

    def validate(self, dataset: Any) -> ValidationResult:
        return ValidationResult(
            passes=dataset.notnull().all(),
            message="No nulls found in dataset",
            diagnostics={
                "num_nulls": len(dataset) - dataset.notnull().sum(),
                "num_rows": len(dataset),
            },
        )


@function_modifiers.check_output_custom(NoNullValidator(importance="fail"))
def dataset() -> pd.DataFrame:
    return ...
