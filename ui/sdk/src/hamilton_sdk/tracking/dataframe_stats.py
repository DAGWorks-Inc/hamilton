from dataclasses import dataclass
from typing import Any, Dict, Union

import numpy as np
import pandas as pd


def type_converter(obj: Any) -> Any:
    # obj = getattr(self, key)
    if isinstance(obj, np.ndarray):
        result = obj.tolist()
    elif isinstance(obj, np.integer):
        result = int(obj)
    elif isinstance(obj, np.floating):
        result = float(obj)
    elif isinstance(obj, np.complex_):
        result = complex(obj)
    elif isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            result[k] = type_converter(v)
    else:
        result = obj
    # nans
    is_null = pd.isnull(result)
    # this ensures we skip evaluating the truthiness of lists/series/arrays
    if isinstance(is_null, bool) and is_null:
        return None
    return result


@dataclass
class BaseColumnStatistics:
    name: str  # Name of the column
    pos: int  # Position in the dataframe. Series will mean 0 position.
    data_type: str
    count: int
    missing: float

    def to_dict(self) -> dict:
        result = {}
        for key, obj in self.__dict__.items():
            result[key] = type_converter(obj)
        return result


@dataclass
class UnhandledColumnStatistics(BaseColumnStatistics):
    base_data_type: str = "unhandled"


@dataclass
class BooleanColumnStatistics(BaseColumnStatistics):
    """Simplified numeric column statistics."""

    zeros: int
    base_data_type: str = "boolean"


@dataclass
class NumericColumnStatistics(BaseColumnStatistics):
    """Inspired by TFDV's ColumnStatistics proto."""

    zeros: int
    min: Union[float, int]
    max: Union[float, int]
    mean: float
    std: float
    quantiles: Dict[float, float]
    histogram: Dict[str, int]
    base_data_type: str = "numeric"


@dataclass
class DatetimeColumnStatistics(NumericColumnStatistics):
    """Placeholder class."""

    base_data_type: str = "datetime"


@dataclass
class CategoryColumnStatistics(BaseColumnStatistics):
    """Inspired by TFDV's ColumnStatistics proto."""

    empty: int
    domain: Dict[str, int]
    top_value: str
    top_freq: int
    unique: int
    base_data_type: str = "category"


@dataclass
class StringColumnStatistics(BaseColumnStatistics):
    """Similar to category, but we don't do domain, top_value, top_freq, unique."""

    avg_str_len: float
    std_str_len: float
    empty: int
    base_data_type: str = "str"
