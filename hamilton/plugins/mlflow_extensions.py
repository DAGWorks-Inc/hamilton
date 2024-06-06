import dataclasses
import pathlib
import shutil
from typing import Any, Callable, Collection, Dict, Literal, Optional, Tuple, Type, Union

try:
    import mlflow
except ImportError:
    raise NotImplementedError("MLFlow is not installed.")

from hamilton import registry
from hamilton.io.data_adapters import DataLoader, DataSaver


@dataclasses.dataclass
class MLFlowModelSaver(DataSaver):
    """
    :param path: Specify a filesystem path or model URI for MLFlow runs or registry
    :param mode: `save` will store to local filesystem; `log` will add to MLFlow registry
    :param flavor: sklearn, xgboost, etc.
    :param run_id: Explicit run id used for `mode=log`. Otherwise, will use active run or create one.
    :param kwargs: additional arguments to pass to `.save_model()` and `.log_model()`.
        They can be flavor-specific.
    """

    path: Union[str, pathlib.Path] = "model"
    mode: Literal["filesystem", "runs"] = "filesystem"
    flavor: Optional[str] = None
    run_id: Optional[str] = None
    overwrite: bool = False
    register: bool = False
    model_name: Optional[str] = None
    kwargs: Optional[Dict[str, Any]] = None
    # kwargs: Dict[str, Any] = dataclasses.field(default_factory=dict)

    # A lot of dancing around because dataclass doesn't accept kwargs
    # and hamilton.function_modifiers.adapters throws `InvalidDecoratorException` for dataclasses.field() defaults
    def __post_init__(self):
        self.kwargs = self.kwargs if self.kwargs else {}

        # ensures that model_name is not None in case register=True
        if self.model_name is None:
            self.model_name = pathlib.Path(self.path).name

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [Any]

    def save_data(self, data) -> Dict[str, Any]:
        if self.flavor:
            flavor = self.flavor
        else:
            # infer the flavor from the base module of the data class
            # for example, extract `sklearn` from `sklearn.linear_model._base`
            flavor, _, _ = data.__module__.partition(".")

        # retrieve the `mlflow.FLAVOR` submodule to use `.save_model()` and `.log_model()`
        try:
            flavor_module = getattr(mlflow, flavor)
        except ImportError:
            raise ImportError(f"Flavor {flavor} is unsupported by MLFlow")

        if self.mode == "filesystem":
            # have to manually delete directory to avoid MLFlow exception
            if self.overwrite is True:
                shutil.rmtree(self.path)

            # .save_model() doesn't return anything
            flavor_module.save_model(data, self.path, **self.kwargs)
            model_info = mlflow.models.get_model_info(self.path)

        elif self.mode == "runs":
            # handle `run_id` and active run conflicts
            if mlflow.active_run() and self.run_id:
                if mlflow.active_run().info.run_id != self.run_id:
                    raise RuntimeError(
                        "The MLFlowModelSaver `run_id` doesn't match the active `run_id`\n",
                        "Leave the `run_id` to None to save to the active MLFlow run.",
                    )

            # save to active run
            if mlflow.active_run():
                model_info = flavor_module.log_model(data, self.path, **self.kwargs)
            # create a run with `run_id` and save to it
            else:
                with mlflow.start_run(run_id=self.run_id):
                    model_info = flavor_module.log_model(data, self.path, **self.kwargs)

        metadata = {k.strip("_"): v for k, v in model_info.__dict__.items()}
        if self.register:
            model_version = mlflow.register_model(
                model_uri=metadata["model_uri"], name=self.model_name
            )
            metadata["registered_model"] = {
                k.strip("_"): v for k, v in model_version.__dict__.items()
            }

        return metadata


# TODO handle loading from file, run, or registry


@dataclasses.dataclass
class MLFlowModelLoader(DataLoader):
    flavor: str
    path: Union[str, pathlib.Path] = "model"
    model_uri: Optional[str] = None
    mode: Literal["filesystem", "runs", "registry"] = "filesystem"
    run_id: Optional[str] = None
    model_name: Optional[str] = None
    version: Union[str, int] = "latest"
    kwargs: Optional[Dict[str, Any]] = None
    # kwargs: Dict[str, Any] = dataclasses.field(default_factory=dict)

    # A lot of dancing around because dataclass doesn't accept kwargs
    # and hamilton.function_modifiers.adapters throws `InvalidDecoratorException` for dataclasses.field() defaults
    def __post_init__(self):
        self.kwargs = self.kwargs if self.kwargs else {}

        if self.model_uri:
            return

        if self.mode == "filesystem":
            self.model_uri = pathlib.Path(self.path).as_uri()
        elif self.mode == "runs":
            self.model_uri = f"runs:/{self.run_id}/{self.path}"
        elif self.mode == "registry":
            self.model_uri = f"models:/{self.model_name}/{self.version}"

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [Callable]

    def load_data(self, type_: Type) -> Tuple[Any, Dict[str, Any]]:
        try:
            flavor_module = getattr(mlflow, self.flavor)
        except ImportError:
            raise ImportError(f"Flavor {self.flavor} is unsupported by MLFlow")

        model = flavor_module.load_model(model_uri=self.model_uri)
        model_info = mlflow.models.model.get_model_info(self.model_uri)
        metadata = {k.strip("_"): v for k, v in model_info.__dict__.items()}
        return model, metadata


def register_data_loaders():
    """Function to register the data loaders for this extension."""
    for loader in [
        MLFlowModelSaver,
        MLFlowModelLoader,
    ]:
        registry.register_adapter(loader)


register_data_loaders()

COLUMN_FRIENDLY_DF_TYPE = False
