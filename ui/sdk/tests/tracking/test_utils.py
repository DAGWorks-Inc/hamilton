from hamilton_sdk.tracking import utils
from enum import Enum
import datetime
import pandas as pd
import numpy as np
import dataclasses


class TestEnum(Enum):
    VALUE1 = "value1"
    VALUE2 = "value2"


@dataclasses.dataclass
class TestDataclass:
    field1: str
    field2: int


def test_make_json_safe_with_dict():
    input_dict = {"key1": "value1", "key2": datetime.datetime.now()}
    result = utils.make_json_safe(input_dict)
    assert isinstance(result, dict)
    assert isinstance(result["key2"], str)


def test_make_json_safe_with_list():
    input_list = ["value1", datetime.datetime.now()]
    result = utils.make_json_safe(input_list)
    assert isinstance(result, list)
    assert isinstance(result[1], str)


def test_make_json_safe_with_numpy_array():
    input_array = np.array([1, 2, 3])
    result = utils.make_json_safe(input_array)
    assert isinstance(result, list)


def test_make_json_safe_with_datetime():
    input_datetime = datetime.datetime.now()
    result = utils.make_json_safe(input_datetime)
    assert isinstance(result, str)


def test_make_json_safe_with_dataclass():
    input_dataclass = TestDataclass("value1", 2)
    result = utils.make_json_safe(input_dataclass)
    assert isinstance(result, dict)


def test_make_json_safe_with_enum():
    input_enum = TestEnum.VALUE1
    result = utils.make_json_safe(input_enum)
    assert result == "value1"


def test_make_json_safe_with_object_having_to_dict():
    class TestClass:
        def to_dict(self):
            return {"field1": "value1", "field2": 2}

    input_object = TestClass()
    result = utils.make_json_safe(input_object)
    assert isinstance(result, dict)


def test_make_json_safe_with_json_serializable_types():
    input_str = "value1"
    result = utils.make_json_safe(input_str)
    assert result == "value1"

    input_int = 2
    result = utils.make_json_safe(input_int)
    assert result == 2

    input_float = 2.0
    result = utils.make_json_safe(input_float)
    assert result == 2.0

    input_bool = True
    result = utils.make_json_safe(input_bool)
    assert result is True


def test_make_json_safe_with_pandas_dataframe():
    input_dataframe = pd.DataFrame(
        {
            "A": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype=np.float64),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )
    utils.make_json_safe(input_dataframe)


def test_make_json_safe_with_pandas_series():
    input_series = pd.Series(["a", "b", "c", "d"])
    utils.make_json_safe(input_series)


def test_make_json_safe_with_file_object(tmp_path):
    with open(tmp_path / "test.txt", "w") as f:
        f.write("test")
        result = utils.make_json_safe(f)
    assert isinstance(result, str)
