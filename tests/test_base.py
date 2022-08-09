import collections
import typing

import numpy as np
import pandas as pd
from numpy import testing
import pytest

from hamilton import base


def test_numpymatrixresult_int():
    """Tests the happy path of build_result of numpymatrixresult"""
    outputs = collections.OrderedDict(
        a=np.array([1, 7, 3, 7, 3, 6, 4, 9, 5, 0]),
        b=np.zeros(10),
        c=1
    )
    expected = np.array([[1, 7, 3, 7, 3, 6, 4, 9, 5, 0],
                         np.zeros(10),
                         np.ones(10)]).T
    actual = base.NumpyMatrixResult().build_result(**outputs)
    testing.assert_array_equal(actual, expected)


def test_numpymatrixresult_raise_length_mismatch():
    """Test raising an error build_result of numpymatrixresult"""
    outputs = collections.OrderedDict(
        a=np.array([1, 7, 3, 7, 3, 6, 4, 9, 5, 0]),
        b=np.array([1, 2, 3, 4, 5]),
        c=1
    )
    with pytest.raises(ValueError):
        base.NumpyMatrixResult().build_result(**outputs)


def test_SimplePythonGraphAdapter():
    """Tests that it delegates as intended"""
    class Foo(base.ResultMixin):
        @staticmethod
        def build_result(**outputs: typing.Dict[str, typing.Any]) -> typing.Any:
            outputs.update({'esoteric': 'function'})
            return outputs
    spga = base.SimplePythonGraphAdapter(Foo())
    cols = {'a': 'b'}
    expected = {'a': 'b', 'esoteric': 'function'}
    actual = spga.build_result(**cols)
    assert actual == expected


T = typing.TypeVar('T')


@pytest.mark.parametrize('node_type,input_value', [
    (typing.Any, None),
    (pd.Series, pd.Series([1, 2, 3])),
    (T, None),
    (typing.List, []),
    (typing.Dict, {}),
    (dict, {}),
    (list, []),
    (int, 1),
    (float, 1.0),
    (str, 'abc'),
    (typing.Union[int, pd.Series], pd.Series([1,2,3])),
    (typing.Union[int, pd.Series], 1),
    (typing.Union[int, typing.Union[float, pd.Series]], 1.0),
], ids=[
    'test-any',
    'test-subclass',
    'test-typevar',
    'test-generic-list',
    'test-generic-dict',
    'test-type-match-dict',
    'test-type-match-list',
    'test-type-match-int',
    'test-type-match-float',
    'test-type-match-str',
    'test-union-match-series',
    'test-union-match-int',
    'test-union-match-nested-float',
])
def test_SimplePythonDataFrameGraphAdapter_check_input_type_match(node_type, input_value):
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    adapter = base.SimplePythonDataFrameGraphAdapter()
    actual = adapter.check_input_type(node_type, input_value)
    assert actual is True


@pytest.mark.parametrize('node_type,input_value', [
    (pd.DataFrame, pd.Series([1, 2, 3])),
    (typing.List, {}),
    (typing.Dict, []),
    (dict, []),
    (list, {}),
    (int, 1.0),
    (float, 1),
    (str, 0),
    (typing.Union[int, pd.Series], pd.DataFrame({'a': [1, 2, 3]})),
    (typing.Union[int, pd.Series], 1.0),
], ids=[
    'test-subclass',
    'test-generic-list',
    'test-generic-dict',
    'test-type-match-dict',
    'test-type-match-list',
    'test-type-match-int',
    'test-type-match-float',
    'test-type-match-str',
    'test-union-mismatch-dataframe',
    'test-union-mismatch-float',
])
def test_SimplePythonDataFrameGraphAdapter_check_input_type_mismatch(node_type, input_value):
    """Tests check_input_type of SimplePythonDataFrameGraphAdapter"""
    adapter = base.SimplePythonDataFrameGraphAdapter()
    actual = adapter.check_input_type(node_type, input_value)
    assert actual is False


@pytest.mark.parametrize('node_type,input_type', [
    (typing.Any, typing.Any),
    (pd.Series, pd.Series),
    (T, T),
    (typing.List, typing.List),
    (typing.Dict, typing.Dict),
    (dict, dict),
    (list, list),
    (int, int),
    (float, float),
    (str, str),
    (typing.Union[int, pd.Series], typing.Union[int, pd.Series]),
    (pd.Series, typing.Union[int, pd.Series]),
    (int, typing.Union[int, pd.Series]),
    (float, typing.Union[int, typing.Union[float, pd.Series]]),
], ids=[
    'test-any',
    'test-subclass',
    'test-typevar',
    'test-generic-list',
    'test-generic-dict',
    'test-type-match-dict',
    'test-type-match-list',
    'test-type-match-int',
    'test-type-match-float',
    'test-type-match-str',
    'test-union-match-exact',
    'test-union-match-subset-series',
    'test-union-match-subset-int',
    'test-union-match-subset-nested-float',
])
def test_SimplePythonDataFrameGraphAdapter_check_node_type_equivalence_match(node_type, input_type):
    """Tests matches for check_node_type_equivalence function"""
    adapter = base.SimplePythonDataFrameGraphAdapter()
    actual = adapter.check_node_type_equivalence(node_type, input_type)
    assert actual is True


@pytest.mark.parametrize('node_type,input_type', [
    (typing.Union[int, pd.Series], typing.Any),
    (pd.DataFrame, pd.Series),
    (typing.List, list),
    (typing.Dict, dict),
    (dict, list),
    (list, dict),
    (int, float),
    (float, int),
    (str, int),
    (typing.Union[int, pd.Series], float),
], ids=[
    'test-any-mismatch',
    'test-class-mismatch',
    'test-generic-mismatch-list',
    'test-generic-mismatch-dict',
    'test-type-mistmatch-dict',
    'test-type-mismatch-list',
    'test-type-mismatch-int',
    'test-type-mismatch-float',
    'test-type-mismatch-str',
    'test-union-mismatch-float',
])
def test_SimplePythonDataFrameGraphAdapter_check_node_type_equivalence_mismatch(node_type, input_type):
    """Tests mismatches for check_node_type_equivalence function"""
    adapter = base.SimplePythonDataFrameGraphAdapter()
    actual = adapter.check_node_type_equivalence(node_type, input_type)
    assert actual is False


@pytest.mark.parametrize('outputs,expected_result', [
    ({'a': pd.Series([1, 2, 3])},
     pd.DataFrame({'a': pd.Series([1, 2, 3])})),
    ({'a': pd.DataFrame({'a': [1, 2, 3], 'b': [11, 12, 13]})},
     pd.DataFrame({'a': pd.Series([1, 2, 3]),
                   'b': pd.Series([11, 12, 13])})),
    ({'a': pd.Series([1, 2, 3]),
      'b': pd.Series([11, 12, 13])},
     pd.DataFrame({'a': pd.Series([1, 2, 3]),
                   'b': pd.Series([11, 12, 13])})),
    ({'a': pd.Series([1, 2, 3]),
      'b': pd.Series([11, 12, 13]),
      'c': 1},
     pd.DataFrame({'a': pd.Series([1, 2, 3]),
                   'b': pd.Series([11, 12, 13]),
                   'c': pd.Series([1, 1, 1])})),
], ids=[
    'test-single-series',
    'test-single-dataframe',
    'test-multiple-series',
    'test-multiple-series-with-scalar',
])
def test_PandasDataFrameResult_build_result(outputs, expected_result):
    """Tests the happy case of PandasDataFrameResult.build_result()"""
    pdfr = base.PandasDataFrameResult()
    actual = pdfr.build_result(**outputs)
    pd.testing.assert_frame_equal(actual, expected_result)


@pytest.mark.parametrize('outputs', [
    ({'a': 1}),
    ({'a': pd.DataFrame({'a': [1, 2, 3], 'b': [11, 12, 13]}),
      'b': pd.DataFrame({'c': [1, 3, 5], 'd': [14, 15, 16]})}),
    ({'a': pd.Series([1, 2, 3]),
      'b': pd.Series([11, 12, 13]),
      'c': pd.DataFrame({'d': [0, 0, 0]})}),
], ids=[
    'test-single-value',
    'test-multiple-dataframes',
    'test-multiple-series-with-dataframe',
])
def test_PandasDataFrameResult_build_result_errors(outputs):
    """Tests the happy case of PandasDataFrameResult.build_result()"""
    pdfr = base.PandasDataFrameResult()
    with pytest.raises(ValueError):
        pdfr.build_result(**outputs)
