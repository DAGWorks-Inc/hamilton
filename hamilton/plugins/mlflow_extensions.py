import dataclasses
import pathlib
from typing import Any, Collection, Dict, Literal, Optional, Tuple, Type, Union

try:
    import mlflow
except ImportError:
    raise NotImplementedError("MLFlow is not installed.")

from hamilton import registry
from hamilton.io.data_adapters import DataLoader, DataSaver


@dataclasses.dataclass
class MLFlowModelSaver(DataSaver):
    """Save model to the MLFlow tracking server using `.log_model()`

    :param path: Run relative path to store model. Will constitute the model URI.
    :param register_as: If not None, register the model under the specified name.
    :param flavor: Library format to save the model (sklearn, xgboost, etc.). Automatically inferred if None.
    :param run_id: Log model to a specific run. Leave to `None` if using the `MLFlowTracker`
    :param kwargs: Arguments for `.log_model()`. Can be flavor-specific.
    """

    path: Union[str, pathlib.Path] = "model"
    register_as: Optional[str] = None
    flavor: Optional[str] = None
    run_id: Optional[str] = None
    kwargs: Dict[str, Any] = None

    def __post_init__(self):
        self.kwargs = self.kwargs if self.kwargs else {}

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

        # retrieve the `mlflow.FLAVOR` submodule to `.log_model()`
        try:
            flavor_module = getattr(mlflow, flavor)
        except ImportError:
            raise ImportError(f"Flavor {flavor} is unsupported by MLFlow")

        # handle `run_id` and active run conflicts
        if mlflow.active_run() and self.run_id:
            if mlflow.active_run().info.run_id != self.run_id:
                raise RuntimeError(
                    "The MLFlowModelSaver `run_id` doesn't match the active `run_id`\n",
                    "Set `run_id=None` to save to the active MLFlow run.",
                )

        # save to active run
        if mlflow.active_run():
            model_info = flavor_module.log_model(data, self.path, **self.kwargs)
        # create a run with `run_id` and save to it
        else:
            with mlflow.start_run(run_id=self.run_id):
                model_info = flavor_module.log_model(data, self.path, **self.kwargs)

        # create metadata from ModelInfo object
        metadata = {k.strip("_"): v for k, v in model_info.__dict__.items()}

        if self.register_as:
            model_version = mlflow.register_model(
                model_uri=metadata["model_uri"], name=self.register_as
            )
            # update metadata with the registered ModelVersion
            # there's a contract between this key and the MLFlowTracker's
            # post_node_execute() reads this metadata
            metadata["registered_model"] = {
                k.strip("_"): v for k, v in model_version.__dict__.items()
            }

        return metadata


@dataclasses.dataclass
class MLFlowModelLoader(DataLoader):
    """Load model from the MLFlow tracking server or model registry using .load_model()
    You can pass a model URI or the necessary metadata to retrieve the model

    :param model_uri: Model location starting as `runs:/` for tracking or `models:/` for registry
    :param mode: `tracking` or registry`. tracking needs `run_id` and `path`. registry needs `model_name` and `version` or `version_alias`.
    :param run_id: Run id of the model on the tracking server
    :param path: Run relative path where the model is stored
    :param model_name: Name of the registered model (equivalent to `register_as` in model saver)
    :param version: Version of the registered model. Can pass as string `v1` or integer `1`
    :param version_alias: Version alias of the registered model. Specify either this or `version`
    :param flavor: Library format to load the model (sklearn, xgboost, etc.). Automatically inferred if None.
    :param kwargs: Arguments for `.load_model()`. Can be flavor-specific.
    """

    model_uri: Optional[str] = None
    mode: Literal["tracking", "registry"] = "tracking"
    run_id: Optional[str] = None
    path: Union[str, pathlib.Path] = "model"
    model_name: Optional[str] = None
    version: Optional[Union[str, int]] = None
    version_alias: Optional[str] = None
    flavor: Optional[str] = None
    kwargs: Dict[str, Any] = None

    # __post_init__ is required to set kwargs as empty dict because
    # can't set: kwargs: Dict[str, Any] = dataclasses.field(default_factory=dict)
    # otherwise raises `InvalidDecoratorException` because materializer factory check
    # for all params being set and `kwargs` would be unset until instantiation.
    def __post_init__(self):
        self.kwargs = self.kwargs if self.kwargs else {}

        if self.model_uri:
            return

        if self.mode == "tracking":
            if (not self.run_id) or (not self.path):
                raise ValueError("Using `mode='tracking'` requires passing `run_id` and `path`")

            self.model_uri = f"runs:/{self.run_id}/{self.path}"

        elif self.mode == "registry":
            if not self.model_name:
                raise ValueError("Using `mode='registry` requires passing `model_name`")

            if not (bool(self.version) ^ bool(self.version_alias)):
                raise ValueError(
                    "If using `mode='registry'` requires passing `version` OR `version_alias"
                )

            if self.version:
                self.model_uri = f"models:/{self.model_name}/{self.version}"
            elif self.version_alias:
                self.model_uri = f"models:/{self.model_name}@{self.version_alias}"

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [Any]

    def load_data(self, type_: Type) -> Tuple[Any, Dict[str, Any]]:
        model_info = mlflow.models.model.get_model_info(self.model_uri)
        metadata = {k.strip("_"): v for k, v in model_info.__dict__.items()}

        flavor = self.flavor
        # if flavor not explicitly passed, retrieve flavor from the ModelInfo
        if flavor is None:
            # prioritize the library specific flavor. Default to `pyfunc` if none available.
            try:
                flavor = next(f for f in metadata["flavors"].keys() if f != "python_function")
            except StopIteration:
                flavor = "pyfunc"

        # retrieve the `mlflow.FLAVOR` submodule to `.log_model()`
        try:
            flavor_module = getattr(mlflow, flavor)
        except ImportError:
            raise ImportError(f"Flavor {flavor} is unsupported by MLFlow")

        model = flavor_module.load_model(model_uri=self.model_uri)
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
