import pytest

from hamilton import driver
from hamilton.lifecycle import default

from tests.resources import mismatched_types


def test_noedge_input_type_checking_without_adapter():
    with pytest.raises(ValueError):
        driver.Builder().with_modules(mismatched_types).build()


def test_noedge_input_type_checking_with_adapter():
    dr = (
        driver.Builder()
        .with_modules(mismatched_types)
        .with_adapters(default.NoEdgeAndInputTypeChecking())
        .build()
    )
    actual = dr.execute(["baz"], inputs={"a": 1.02, "number": "aaasdfdsf"})
    assert actual == {"baz": "1.02 2 aaasdfdsf"}
