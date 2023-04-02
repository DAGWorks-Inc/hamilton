import dataclasses
import json
import os
import pickle
from typing import Any, Dict, Tuple, Type

from hamilton.htypes import custom_subclass_check
from hamilton.io.data_loaders import DataLoader, LoadType
from hamilton.io.utils import get_file_loading_metadata


@dataclasses.dataclass
class JSONDataLoader(DataLoader):
    path: str

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return custom_subclass_check(type_, dict)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        with open(self.path, "r") as f:
            return json.load(f), get_file_loading_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "json"


@dataclasses.dataclass
class LiteralValueDataLoader(DataLoader):
    value: Any

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return True

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        return self.value, {}

    @classmethod
    def name(cls) -> str:
        return "literal"


@dataclasses.dataclass
class RawFileDataLoader(DataLoader):
    path: str
    encoding: str = "utf-8"

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return custom_subclass_check(type_, str)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        with open(self.path, "r", encoding=self.encoding) as f:
            return f.read(), get_file_loading_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "file"


@dataclasses.dataclass
class PickleLoader(DataLoader):
    path: str

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return True  # no way to know beforehand

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        with open(self.path, "rb") as f:
            return pickle.load(f), get_file_loading_metadata(self.path)

    @classmethod
    def name(cls) -> str:
        return "pickle"


@dataclasses.dataclass
class EnvVarDataLoader(DataLoader):

    names: Tuple[str, ...]

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return custom_subclass_check(type_, dict)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        return {name: os.environ[name] for name in self.names}, {}

    @classmethod
    def name(cls) -> str:
        return "environment"


DATA_LOADERS = [
    JSONDataLoader,
    LiteralValueDataLoader,
    RawFileDataLoader,
    PickleLoader,
    EnvVarDataLoader,
]
