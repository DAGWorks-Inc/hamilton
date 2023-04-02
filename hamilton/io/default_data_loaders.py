import dataclasses
import json
import os
import pickle
from typing import Any, Collection, Dict, Tuple, Type

from hamilton.io.data_loaders import DataLoader
from hamilton.io.utils import get_file_loading_metadata


@dataclasses.dataclass
class JSONDataLoader(DataLoader):
    path: str

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [dict]

    def load_data(self, type_: Type) -> Tuple[dict, Dict[str, Any]]:
        with open(self.path, "r") as f:
            return json.load(f), get_file_loading_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "json"


@dataclasses.dataclass
class LiteralValueDataLoader(DataLoader):
    value: Any

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [Any]

    def load_data(self, type_: Type) -> Tuple[dict, Dict[str, Any]]:
        return self.value, {}

    @classmethod
    def name(cls) -> str:
        return "literal"


@dataclasses.dataclass
class RawFileDataLoader(DataLoader):
    path: str
    encoding: str = "utf-8"

    def load_data(self, type_: Type) -> Tuple[str, Dict[str, Any]]:
        with open(self.path, "r", encoding=self.encoding) as f:
            return f.read(), get_file_loading_metadata(self.path)

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [str]

    @classmethod
    def name(cls) -> str:
        return "file"


@dataclasses.dataclass
class PickleLoader(DataLoader):
    path: str

    def load_data(self, type_: Type[dict]) -> Tuple[str, Dict[str, Any]]:
        with open(self.path, "rb") as f:
            return pickle.load(f), get_file_loading_metadata(self.path)

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [str]

    @classmethod
    def name(cls) -> str:
        return "pickle"


@dataclasses.dataclass
class EnvVarDataLoader(DataLoader):
    names: Tuple[str, ...]

    def load_data(self, type_: Type[dict]) -> Tuple[dict, Dict[str, Any]]:
        return {name: os.environ[name] for name in self.names}, {}

    @classmethod
    def name(cls) -> str:
        return "environment"

    @classmethod
    def load_targets(cls) -> Collection[Type]:
        return [dict]


DATA_LOADERS = [
    JSONDataLoader,
    LiteralValueDataLoader,
    RawFileDataLoader,
    PickleLoader,
    EnvVarDataLoader,
]
