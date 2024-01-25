import dataclasses
from typing import Any, Collection, Dict, Tuple, Type

import pandas as pd
from pandas import DataFrame

from hamilton import driver, registry
from hamilton.lifecycle import NodeExecutionHook


# preamble
class Model:
    pass


def _my_custom_loading_function(uri) -> pd.DataFrame:
    pass


other_metadata = {}


# lifecycle_adapters 0

# code


class ExampleHook(NodeExecutionHook):
    def run_before_node_execution(self, *, node_name: str, **future_kwargs: Any):
        print(f"running: {node_name}")

    def run_after_node_execution(self, *, node_name: str, **future_kwargs: Any):
        print(f"ran: {node_name}")


dr = driver.Builder().with_modules(...).with_adapters(ExampleHook())
dr.execute(...)

# data_loaders 0

from hamilton.function_modifiers import load_from, save_to, source, tag


@load_from.csv(path=source("training_data_path"))
def trained_model(training_data: pd.DataFrame, hyperparameters: dict) -> Model:
    return Model(**hyperparameters).train(training_data)


@save_to.parquet(path="./test.parquet", id="save_predictions")
def test_predictions(trained_model: Model, test_features: pd.DataFrame) -> pd.DataFrame:
    return trained_model.predict(test_features)


# data_loaders 1
from hamilton.io.materialization import from_, to

dr = driver.Builder().with_modules(...).build()

dr.materialize(
    from_.csv(target="training_data", path=source("training_data_path")),
    to.parquet(path="./test.parquet", id="save_predictions", dependencies=["test_predictions"]),
)

# TODO -- mention source versus normal one, ID

from hamilton import registry

# data_loaders 2
from hamilton.io import materialization


@dataclasses.dataclass
class CustomDataLoader(materialization.DataLoader):
    """Custom data loader that takes in a URI"""

    uri: str

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [pd.DataFrame]

    @classmethod
    def name(cls) -> str:
        return "my_custom_loader"

    def load_data(self, type_: Type[Type]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        return _my_custom_loading_function(self.uri), {"uri": self.uri, **other_metadata}


registry.register_adapter(CustomDataLoader)

dr.materialize(from_.my_custom_loader(uri="...", target="input_name_of_data_to_load"))


# TODO -- mention the name as the referring one,

# tagging 0


@tag(
    owner="my_team_name",
    region=["US", "UK"],
    compliance_properties=["pii"],
    data_produce="my_data_product",
)
def some_dataset(input_data: DataFrame) -> DataFrame:
    ...
