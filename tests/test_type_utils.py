import pandas as pd
import pytest
import typing

from hamilton import base, type_utils


class X:
    pass


class Y(X):
    pass


custom_type = typing.TypeVar('FOOBAR')


@pytest.mark.parametrize('param_type,required_type,expected', [
    (custom_type, custom_type, True),
    (custom_type, typing.TypeVar('FOO'), False),
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
])
def test_custom_subclass_check(param_type, required_type, expected):
    """Tests the custom_subclass_check"""
    actual = type_utils.custom_subclass_check(required_type, param_type)
    assert actual == expected


class TestAdapter(base.SimplePythonDataFrameGraphAdapter):
    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        # fake equivalence function
        return node_type == pd.Series and input_type == list


adapter = TestAdapter()


@pytest.mark.parametrize('adapter,param_type,required_type,expected', [
    (None, typing.TypeVar('FOO'), typing.TypeVar('BAR'), False),
    (None, custom_type, custom_type, True),
    (None, int, int, True),
    (adapter, int, float, False),
    (None, typing.Dict, typing.Any, True),
    (None, X, X, True),
    (None, X, Y, True),
    (adapter, pd.Series, pd.Series, True),
    (adapter, list, pd.Series, True),
    (adapter, dict, pd.Series, False),
])
def test_types_match(adapter, param_type, required_type, expected):
    """Tests the types_match function"""
    actual = type_utils.types_match(adapter, param_type, required_type)
    assert actual == expected
