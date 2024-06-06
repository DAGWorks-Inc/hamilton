import dataclasses
import pathlib
from typing import Any, Callable, Collection, Dict, Literal, Optional, Tuple, Type, Union

try:
    import mlflow
except ImportError:
    raise NotImplementedError("MLFlow is not installed.")

from hamilton import registry
from hamilton.io.data_adapters import DataLoader, DataSaver


@dataclasses.dataclass
class MLFlowModelSaver(DataSaver):
    def __init__(
        self,
        path: Union[str, pathlib.Path] = "model",
        mode: Literal["save", "log"] = "save",
        flavor: Optional[str] = None,
        run_id: Optional[str] = None,
        **kwargs,
    ):
        """
        :param path: Specify a filesystem path or model URI for MLFlow runs or registry
        :param mode: `save` will store to local filesystem; `log` will add to MLFlow registry
        :param flavor: sklearn, xgboost, etc.
        :param run_id: Explicit run id used for `mode=log`. Otherwise, will use active run or create one.
        :param kwargs: additional arguments to pass to `.save_model()` and `.log_model()`.
            They can be flavor-specific.
        """
        self.path = path
        self.mode = mode
        self.flavor = flavor
        self.run_id = run_id
        self.kwargs = kwargs

    @classmethod
    def name(cls) -> str:
        return "mlflow"

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [Callable]

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

        if self.mode == "save":
            # .save_model() doesn't return anything
            flavor_module.save_model(data, self.path, **self.kwargs)
            metadata = dict(path=self.path, mode="save", flavor=flavor, **self.kwargs)

        elif self.mode == "log":
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
        return metadata


# TODO handle loading from file, run, or registry


@dataclasses.dataclass
class MLFlowModelLoader(DataLoader):
    def __init__(
        self,
        flavor: str,
        path: Union[str, pathlib.Path] = "model",
        model_uri: Optional[str] = None,
        mode: Literal["filesystem", "runs", "registry"] = "filesystem",
        run_id: Optional[str] = None,
        model_name: Optional[str] = None,
        version: Union[str, int] = "latest",
        **kwargs,
    ):
        """ """
        self.flavor = flavor
        self.path = path
        self.model_uri = model_uri
        self.mode = mode
        self.run_id = run_id
        self.model_name = model_name
        self.version = version
        self.kwargs = kwargs

        # if self.model_uri:
        # if "runs:/" in self.model_uri:
        #     self.mode = "runs"
        #     # extract info from run model_uri
        #     _, _, remainder = self.model_uri.partition("runs:/")
        #     run_id, _, inferred_path = remainder.partition("/")
        #     self.run_id = run_id
        #     self.path = inferred_path

        # elif "models:/" in self.model_uri:
        #     self.mode = "registry"
        #     # extract info from registry model_uri
        #     _, _, remainder = self.model_uri.partition("models:/")
        #     model_name, _, version = remainder.partition("/")
        #     self.model_name = model_name
        #     self.model_version = version
        if not self.model_uri:
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
