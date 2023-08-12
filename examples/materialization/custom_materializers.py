import dataclasses
import pickle
from typing import Any, Collection, Dict, Type

import numpy as np
from sklearn import base

from hamilton import registry
from hamilton.io import utils
from hamilton.io.data_adapters import DataSaver

# TODO -- put this back in the standard library


@dataclasses.dataclass
class NumpyMatrixToCSV(DataSaver):
    path: str
    sep: str = ","

    def __post_init__(self):
        if not self.path.endswith(".csv"):
            raise ValueError(f"CSV files must end with .csv, got {self.path}")

    def save_data(self, data: np.ndarray) -> Dict[str, Any]:
        np.savetxt(self.path, data, delimiter=self.sep)
        return utils.get_file_metadata(self.path)

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [np.ndarray]

    @classmethod
    def name(cls) -> str:
        return "csv"


@dataclasses.dataclass
class SKLearnPickler(DataSaver):
    path: str

    def save_data(self, data: base.ClassifierMixin) -> Dict[str, Any]:
        pickle.dump(data, open(self.path, "wb"))
        return utils.get_file_metadata(self.path)

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [base.ClassifierMixin]

    @classmethod
    def name(cls) -> str:
        return "pickle"


for adapter in [NumpyMatrixToCSV, SKLearnPickler]:
    registry.register_adapter(adapter)
