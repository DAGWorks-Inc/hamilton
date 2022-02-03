import collections

import numpy as np
from numpy import testing
import pytest

from hamilton import base


def test_numpymatrixresult_int():
    """Tests the happy path of build_result of numpymatrixresult"""
    columns = collections.OrderedDict(
        a=np.array([1, 7, 3, 7, 3, 6, 4, 9, 5, 0]),
        b=np.zeros(10),
        c=1
    )
    expected = np.array([[1, 7, 3, 7, 3, 6, 4, 9, 5, 0],
                         np.zeros(10),
                         np.ones(10)]).T
    actual = base.NumpyMatrixResult().build_result(**columns)
    testing.assert_array_equal(actual, expected)


def test_numpymatrixresult_raise_length_mismatch():
    """Test raising an error build_result of numpymatrixresult"""
    columns = collections.OrderedDict(
        a=np.array([1, 7, 3, 7, 3, 6, 4, 9, 5, 0]),
        b=np.array([1, 2, 3, 4, 5]),
        c=1
    )
    with pytest.raises(ValueError):
        base.NumpyMatrixResult().build_result(**columns)
