import collections
import sys
import typing

import pandas as pd
import pytest

from hamilton import htypes


class X:
    pass


class Y(X):
    pass


custom_type = typing.TypeVar("FOOBAR")


@pytest.mark.parametrize(
    "param_type,requested_type,expected",
    [
        (custom_type, custom_type, True),
        (custom_type, typing.TypeVar("FOO"), False),
        (typing.Any, typing.TypeVar("FOO"), True),
        (typing.Any, custom_type, True),
        (int, int, True),
        (int, float, False),
        (typing.List[int], typing.List, True),
        (typing.List, typing.List[float], True),
        (typing.List, list, True),
        (typing.Dict, dict, True),
        (dict, typing.Dict, True),
        (list, typing.List, True),
        (list, typing.List, True),
        (typing.List[int], typing.List[float], False),
        (typing.Dict, typing.List, False),
        (typing.Mapping, typing.Dict, True),
        (typing.Mapping, dict, True),
        (dict, typing.Mapping, False),
        (typing.Dict, typing.Mapping, False),
        (typing.Iterable, typing.List, True),
        (typing.Tuple[str, str], typing.Tuple[str, str], True),
        (typing.Tuple[str, str], typing.Tuple[str], False),
        (typing.Tuple[str, str], typing.Tuple, True),
        (typing.Tuple, typing.Tuple[str, str], True),
        (typing.Union[str, str], typing.Union[str, str], True),
        (X, X, True),
        (X, Y, True),
        (Y, X, False),
        (typing.Any, Y, True),
        (Y, typing.Any, False),
        (typing.Union[X, int], X, True),
        (typing.Union[str, X], str, True),
        (typing.Union[custom_type, X], Y, True),
        (typing.Union[float, str], int, False),
        (typing.Union[int, float], X, False),
        (collections.Counter, collections.Counter, True),
        (dict, collections.Counter, True),
        (typing.Dict, collections.Counter, True),
        # These are not subclasses of each other, see issue 42
        (typing.FrozenSet[int], typing.Set[int], False),
        (htypes.column[pd.Series, int], pd.Series, True),
        (htypes.column[pd.Series, int], int, False),
        (typing.Any, pd.DataFrame, True),
        (pd.DataFrame, typing.Any, False),
    ],
)
def test_custom_subclass_check(param_type, requested_type, expected):
    """Tests the custom_subclass_check"""
    actual = htypes.custom_subclass_check(requested_type, param_type)
    assert actual == expected


@pytest.mark.parametrize(
    "param_type,required_type,expected",
    [
        (typing.TypeVar("FOO"), typing.TypeVar("BAR"), False),
        (custom_type, custom_type, True),
        (int, int, True),
        (int, float, False),
        (typing.Dict, typing.Any, True),
        (X, X, True),
        (X, Y, True),
        (pd.Series, pd.Series, True),
        (list, pd.Series, False),
        (dict, pd.Series, False),
    ],
)
def test_types_match(param_type, required_type, expected):
    """Tests the types_match function"""
    actual = htypes.types_match(param_type, required_type)
    assert actual == expected


@pytest.mark.parametrize(
    "type_",
    [
        int,
        bool,
        float,
        pd.Series,
        pd.DataFrame,
        htypes.column[pd.Series, int],
        htypes.column[pd.Series, float],
        htypes.column[pd.Series, bool],
        htypes.column[pd.Series, str],
    ],
)
def test_validate_types_happy(type_):
    """Tests that validate_types works when the type is valid"""
    htypes.validate_type_annotation(type_)


@pytest.mark.parametrize(
    "type_",
    [
        htypes.column[pd.DataFrame, int],
        htypes.column[pd.DataFrame, float],
        htypes.column[pd.Series, typing.Dict[str, typing.Any]],
    ],
)
def test_validate_types_sad(type_):
    """Tests that validate_types works when the type is valid"""
    with pytest.raises(htypes.InvalidTypeException):
        htypes.validate_type_annotation(type_)


@pytest.mark.parametrize(
    "candidate,type_,expected",
    [
        (int, int, True),
        (int, float, False),
        # Not safe so we return false
        (typing.List[int], typing.List, False),
        (typing.FrozenSet[int], typing.Set[int], False),
        (typing.Dict, dict, False),
    ],
)
def test__safe_subclass(candidate, type_, expected):
    assert htypes._safe_subclass(candidate, type_) == expected


@pytest.mark.parametrize(
    "type_",
    [
        (custom_type),
        (typing.TypeVar("FOO")),
        (typing.Any),
        (int),
        (float),
        (typing.List[int]),
        (typing.List),
        (list),
        (typing.Iterable),
        (typing.Dict),
        (dict),
        (typing.Mapping),
        (collections.Counter),
        (typing.Tuple[str, str]),
        (typing.Tuple[str]),
        (typing.Tuple),
        (typing.Union[str, str]),
        (X),
        (Y),
        (typing.Any),
        (typing.Union[X, int]),
        (typing.Union[str, X]),
        (typing.Union[custom_type, X]),
        (typing.Union[float, str]),
        (typing.Union[int, float]),
        (typing.FrozenSet[int]),
        (typing.Set[int]),
        (pd.Series),
        (htypes.column[pd.Series, int]),
        (pd.DataFrame),
    ],
)
def test_get_type_as_string(type_):
    """Tests the custom_subclass_check"""
    try:
        type_string = htypes.get_type_as_string(type_)  # noqa: F841
    except Exception as e:
        pytest.fail(f"test get_type_as_string raised: {e}")


@pytest.mark.parametrize(
    "node_type,input_value",
    [
        (pd.DataFrame, pd.Series([1, 2, 3])),
        (typing.List, {}),
        (typing.Dict, []),
        (dict, []),
        (list, {}),
        (int, 1.0),
        (float, 1),
        (str, 0),
        (typing.Union[int, pd.Series], pd.DataFrame({"a": [1, 2, 3]})),
        (typing.Union[int, pd.Series], 1.0),
    ],
    ids=[
        "test-subclass",
        "test-generic-list",
        "test-generic-dict",
        "test-type-match-dict",
        "test-type-match-list",
        "test-type-match-int",
        "test-type-match-float",
        "test-type-match-str",
        "test-union-mismatch-dataframe",
        "test-union-mismatch-float",
    ],
)
def test_check_input_type_mismatch(node_type, input_value):
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    actual = htypes.check_input_type(node_type, input_value)
    assert actual is False


T = typing.TypeVar("T")


@pytest.mark.parametrize(
    "node_type,input_value",
    [
        (typing.Any, None),
        (pd.Series, pd.Series([1, 2, 3])),
        (T, None),
        (typing.List, []),
        (typing.Dict, {}),
        (dict, {}),
        (list, []),
        (int, 1),
        (float, 1.0),
        (str, "abc"),
        (typing.Union[int, pd.Series], pd.Series([1, 2, 3])),
        (typing.Union[int, pd.Series], 1),
    ],
    ids=[
        "test-any",
        "test-subclass",
        "test-typevar",
        "test-generic-list",
        "test-generic-dict",
        "test-type-match-dict",
        "test-type-match-list",
        "test-type-match-int",
        "test-type-match-float",
        "test-type-match-str",
        "test-union-match-series",
        "test-union-match-int",
    ],
)
def test_check_input_type_match(node_type, input_value):
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    actual = htypes.check_input_type(node_type, input_value)
    assert actual is True


# We cannot parameterize this as the parameterization cannot be
# included if the
@pytest.mark.skipif(
    sys.version_info < (3, 9, 0),
    reason="Type hinting generics in standard collections " "is only supported in 3.9+",
)
def test_check_input_types_subscripted_generics_dict_str_Any():
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    actual = htypes.check_input_type(dict[str, typing.Any], {})
    assert actual is True


# We cannot parameterize this as the parameterization cannot be
# included if the
@pytest.mark.skipif(
    sys.version_info < (3, 9, 0),
    reason="Type hinting generics in standard collections " "is only supported in 3.9+",
)
def test_check_input_types_subscripted_generics_list_Any():
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    actual = htypes.check_input_type(list[typing.Any], [])
    assert actual is True
