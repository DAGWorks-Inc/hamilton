try:
    import yaml
except ImportError:
    raise NotImplementedError("yaml is not installed and is needed for yaml hamilton plugin")

import dataclasses
import pathlib
from typing import Any, Collection, Dict, Tuple, Type, Union

from hamilton import registry
from hamilton.io.data_adapters import DataLoader, DataSaver
from hamilton.io.utils import get_file_metadata

PrimitiveType = Union[str, int, bool, dict, list]


@dataclasses.dataclass
class YAMLDataLoader(DataLoader):
    path: Union[str, pathlib.Path]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [PrimitiveType]

    @classmethod
    def name(cls) -> str:
        return "yaml"

    def load_data(self, type_: Type) -> Tuple[PrimitiveType, Dict[str, Any]]:
        path = self.path
        if isinstance(self.path, str):
            path = pathlib.Path(self.path)

        with path.open(mode="r") as f:
            return yaml.safe_load(f), get_file_metadata(path)


@dataclasses.dataclass
class YAMLDataSaver(DataSaver):
    path: Union[str, pathlib.Path]

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [PrimitiveType]

    @classmethod
    def name(cls) -> str:
        return "yaml"

    def save_data(self, data: Any) -> Dict[str, Any]:
        path = self.path
        if isinstance(path, str):
            path = pathlib.Path(path)
        with path.open("w") as f:
            yaml.dump(data, f)
        return get_file_metadata(self.path)


COLUMN_FRIENDLY_DF_TYPE = False


def register_data_loaders():
    for materializer in [
        YAMLDataLoader,
        YAMLDataSaver,
    ]:
        registry.register_adapter(materializer)


register_data_loaders()
