import dataclasses
from typing import Any, Collection, Dict, Tuple, Type

from hamilton.io.data_adapters import DataLoader, DataSaver


@dataclasses.dataclass
class MockDataLoader(DataLoader):
    required_param: int
    default_param: int = 1

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [bool]

    def load_data(self, type_: Type) -> Tuple[int, Dict[str, Any]]:
        pass

    @classmethod
    def name(cls) -> str:
        pass


@dataclasses.dataclass
class MockDataLoader2(DataLoader):
    required_param: int
    default_param: int = 1

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [int]

    def load_data(self, type_: Type) -> Tuple[int, Dict[str, Any]]:
        pass

    @classmethod
    def name(cls) -> str:
        pass


@dataclasses.dataclass
class MockDataSaver(DataSaver):
    required_param: int
    default_param: int = 1

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [bool]

    def save_data(self, type_: Type) -> Tuple[int, Dict[str, Any]]:
        pass

    @classmethod
    def name(cls) -> str:
        pass


@dataclasses.dataclass
class MockDataSaver2(DataSaver):
    required_param: int
    default_param: int = 1

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [int]

    def save_data(self, type_: Type) -> Tuple[int, Dict[str, Any]]:
        pass

    @classmethod
    def name(cls) -> str:
        pass


def test_data_loader_get_required_params():
    assert MockDataLoader.get_required_arguments() == {"required_param": int}
    assert MockDataLoader.get_optional_arguments() == {"default_param": int}


def test_loader_applies_to():
    """Tests applies to is doing the right thing for a loader. i.e. A->B where A is the loader."""
    loader = MockDataLoader(1, 2)
    # bool -> int   -- bool is a subclass of int.
    assert loader.applies_to(int) is True
    # bool -> bool   -- bool is a subclass of itself
    assert loader.applies_to(bool) is True

    loader2 = MockDataLoader2(1, 2)
    # int -> int   -- int is a subclass of int.
    assert loader2.applies_to(int) is True
    # int -> bool   -- bool is not a subclass of int
    assert loader2.applies_to(bool) is False


def test_saver_applies_to():
    """Tests applies to is doing the right thing for a saver. i.e. A->B where B is the saver."""
    saver = MockDataSaver(1, 2)
    # int -> bool -- int is not a subclass of bool
    assert saver.applies_to(int) is False
    # bool -> bool  -- bool is a subclas of itself
    assert saver.applies_to(bool) is True

    saver2 = MockDataSaver2(1, 2)
    # int -> int -- int is a subclass of itself
    assert saver2.applies_to(int) is True
    # bool -> int -- bool is a subclass of int
    assert saver2.applies_to(bool) is True
