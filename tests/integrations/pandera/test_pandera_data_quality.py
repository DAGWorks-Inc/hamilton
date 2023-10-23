import sys

import numpy as np
import pandas as pd
import pandera as pa
import pytest

from hamilton import node
from hamilton.data_quality import pandera_validators
from hamilton.function_modifiers import base
from hamilton.plugins import h_pandera


def test_basic_pandera_decorator_dataframe_fails():
    schema = pa.DataFrameSchema(
        {
            "column1": pa.Column(int),
            "column2": pa.Column(float, pa.Check(lambda s: s < -1.2)),
            # you can provide a list of validators
            "column3": pa.Column(
                str,
                [
                    pa.Check(lambda s: s.str.startswith("value")),
                    pa.Check(lambda s: s.str.split("_", expand=True).shape[1] == 2),
                ],
            ),
        },
        index=pa.Index(int),
        strict=True,
    )

    df = pd.DataFrame({"column1": [5, 1, np.nan]})
    validator = pandera_validators.PanderaDataFrameValidator(schema=schema, importance="warn")
    validation_result = validator.validate(df)
    assert not validation_result.passes
    assert (
        "A total of 4 schema errors were found" in validation_result.message
    )  # TODO -- ensure this will stay constant with the contract


def test_basic_pandera_decorator_dataframe_passes():
    schema = pa.DataFrameSchema(
        {
            "column1": pa.Column(int),
            "column2": pa.Column(float, pa.Check(lambda s: s < -1.2)),
            # you can provide a list of validators
            "column3": pa.Column(
                str,
                [
                    pa.Check(lambda s: s.str.startswith("value")),
                    pa.Check(lambda s: s.str.split("_", expand=True).shape[1] == 2),
                ],
            ),
        },
        index=pa.Index(int),
        strict=True,
    )

    df = pd.DataFrame(
        {
            "column1": [5, 1, 0],
            "column2": [-2.0, -1.9, -20.0],
            "column3": ["value_0", "value_1", "value_2"],
        }
    )
    validator = pandera_validators.PanderaDataFrameValidator(schema=schema, importance="warn")
    validation_result = validator.validate(df)
    assert validation_result.passes


def test_basic_pandera_decorator_series_fails():
    schema = pa.SeriesSchema(
        str,
        checks=[
            pa.Check(lambda s: s.str.startswith("foo")),
            pa.Check(lambda s: s.str.endswith("bar")),
            pa.Check(lambda x: len(x) > 3, element_wise=True),
        ],
        nullable=False,
    )
    series = pd.Series(["foobar", "foobox", "foobaz"])
    validator = pandera_validators.PanderaSeriesSchemaValidator(schema=schema, importance="warn")
    validation_result = validator.validate(series)
    assert not validation_result.passes
    assert "A total of 1 schema error" in validation_result.message


def test_basic_pandera_decorator_series_passes():
    schema = pa.SeriesSchema(
        str,
        checks=[
            pa.Check(lambda s: s.str.startswith("foo")),
            pa.Check(lambda s: s.str.endswith("bar")),
            pa.Check(lambda x: len(x) > 3, element_wise=True),
        ],
        nullable=False,
    )
    series = pd.Series(["foobar", "foo_bar", "foo__bar"])
    validator = pandera_validators.PanderaSeriesSchemaValidator(schema=schema, importance="warn")
    validation_result = validator.validate(series)
    assert validation_result.passes


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.9 or higher")
def test_pandera_decorator_creates_correct_validator():
    class OutputSchema(pa.DataFrameModel):
        year: pa.typing.pandas.Series[int] = pa.Field(gt=2000, coerce=True)
        month: pa.typing.pandas.Series[int] = pa.Field(ge=1, le=12, coerce=True)
        day: pa.typing.pandas.Series[int] = pa.Field(ge=0, le=365, coerce=True)

    def foo(should_break: bool = False) -> pa.typing.pandas.DataFrame[OutputSchema]:
        if should_break:
            return pd.DataFrame(
                {
                    "year": ["-2001", "-2002", "-2003"],
                    "month": ["-13", "-6", "120"],
                    "day": ["700", "-156", "367"],
                }
            )
        return pd.DataFrame(
            {
                "year": ["2001", "2002", "2003"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )

    n = node.Node.from_fn(foo)
    validators = h_pandera.check_output().get_validators(n)
    assert len(validators) == 1
    (validator,) = validators
    result_success = validator.validate(n())  # should not fail
    assert result_success.passes
    result_failed = validator.validate(n(should_break=True))  # should fail
    assert not result_failed.passes


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.9 or higher")
def test_pandera_decorator_fails_without_annotation():
    def foo() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "year": ["2001", "2002", "2003"],
                "month": ["3", "6", "12"],
                "day": ["200", "156", "365"],
            }
        )

    n = node.Node.from_fn(foo)
    with pytest.raises(base.InvalidDecoratorException):
        h_pandera.check_output().get_validators(n)
