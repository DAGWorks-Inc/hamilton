import numbers

import pandas as pd
import pytest

from hamilton.data_quality.base import matches_any_type


@pytest.mark.parametrize(
    "type_,applicable_types,matches",
    [
        (int, [int], True),
        (str, [int], False),
        (str, [int, str], True),
        (str, [int, float], False),
        (int, [numbers.Real], True),
        (pd.Series, [pd.Series, pd.DataFrame], True),
    ],
)
def test_matches_any_type(type_, applicable_types, matches):
    assert matches_any_type(type_, applicable_types) == matches
