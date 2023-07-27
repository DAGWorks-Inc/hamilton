import dataclasses
from typing import Any, Collection, Dict, Tuple, Type

from hamilton.io.data_adapters import DataLoader


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


def test_data_loader_get_required_params():
    assert MockDataLoader.get_required_arguments() == {"required_param": int}
    assert MockDataLoader.get_optional_arguments() == {"default_param": int}
