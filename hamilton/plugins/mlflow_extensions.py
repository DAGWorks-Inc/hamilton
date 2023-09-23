import dataclasses
from typing import Any, Collection, Dict, List, Optional, Tuple, Type, Union

import mlflow
from mlflow.models import ModelInputExample, ModelSignature
from mlflow.sklearn import SERIALIZATION_FORMAT_CLOUDPICKLE, SERIALIZATION_FORMAT_PICKLE
from mlflow.tracking._model_registry import DEFAULT_AWAIT_MAX_SLEEP_SECONDS

from hamilton.io.data_adapters import DataLoader, DataSaver

Serialization = Union[SERIALIZATION_FORMAT_CLOUDPICKLE, SERIALIZATION_FORMAT_PICKLE]


@dataclasses.dataclass
class MlFlowSkLearnModelReader(DataLoader):
    """
    TODO: Write documentation
    """

    model_uri: str
    # kwargs
    dst_path: Optional[str] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return []  # TODO: Decide on applicable types

    def _get_loading_kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        if self.dst_path is not None:
            kwargs["dst_path"] = self.dst_path
        return kwargs

    def load_data(self, type: Type) -> Tuple[Type, Dict[str, Any]]:  # TODO: Decide on type
        model = mlflow.sklearn.load_model(self.model_uri, **self._get_loading_kwargs())
        metadata = {}  # TODO: Decide on meta data fn
        return model, metadata

    @classmethod
    def name(cls) -> str:
        return ""  # TODO: Decide on name


@dataclasses.dataclass
class MlFlowSkLearnModelWriter(DataSaver):
    """
    TODO: Write documentation
    """

    sk_model: Union[mlflow.sklearn, mlflow.pyfunc]  # TODO: Finalize type hint
    path: str
    # kwargs
    await_registration_for: Optional[int] = DEFAULT_AWAIT_MAX_SLEEP_SECONDS
    code_paths: List[str] = None
    conda_env: Optional[Union[str, Dict[str, Any]]] = None
    extra_pip_requirements: Optional[Union[List[str], str]] = None
    input_example: Optional[ModelInputExample] = None
    metadata: Dict[str, Any] = None
    pip_requirements: Optional[Union[List[str], str]] = None
    pyfunc_predict_fn: str = "predict"
    registered_model_name: str = None
    serialization_format: Serialization = SERIALIZATION_FORMAT_CLOUDPICKLE
    signature: Optional[ModelSignature] = None

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return []  # TODO: Decide on applicable type

    def _get_saving_kwargs(self):
        kwargs = {}
        if self.await_registration_for is not None:
            kwargs["await_registration_for"] = self.await_registration_for
        if self.code_paths is not None:
            kwargs["code_paths"] = self.code_paths
        if self.conda_env is not None:
            kwargs["conda_env"] = self.conda_env
        if self.extra_pip_requirements is not None:
            kwargs["extra_pip_requirements"] = self.extra_pip_requirements
        if self.input_example is not None:
            kwargs["input_example"] = self.input_example
        if self.metadata is not None:
            kwargs["metadata"] = self.metadata
        if self.pip_requirements is not None:
            kwargs["pip_requirements"] = self.pip_requirements
        if self.pyfunc_predict_fn is not None:
            kwargs["pyfunc_predict_fn"] = self.pyfunc_predict_fn
        if self.registered_model_name is not None:
            kwargs["registered_model_name"] = self.registered_model_name
        if self.serialization_format is not None:
            kwargs["serialization_format"] = self.serialization_format
        if self.signature is not None:
            kwargs["signature"] = self.signature
        return kwargs

    def save_data(self, data) -> Dict[str, Any]:  # TODO: Decide on data type
        data.save_model(self.sk_model, self.path, **self._get_saving_kwargs())
        return {}  # TODO: Decide on metadata to return

    @classmethod
    def name(cls) -> str:
        return ""  # TODO: Decide on name
