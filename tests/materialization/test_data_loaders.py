import dataclasses
from typing import Any, Dict, Tuple, Type

from hamilton.io.data_loaders import DataLoader, LoadType


@dataclasses.dataclass
class MockDataLoader(DataLoader):
    required_param: int
    default_param: int = 1

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return True

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        pass

    @classmethod
    def name(cls) -> str:
        pass


def test_data_loader_get_required_params():
    assert MockDataLoader.get_required_arguments() == {"required_param": int}
    assert MockDataLoader.get_optional_arguments() == {"default_param": int}
