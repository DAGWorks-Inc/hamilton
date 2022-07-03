from hamilton.data_quality import pandera_validators

import pandera as pa
import pandas as pd
import numpy as np


def test_basic_pandera_decorator_dataframe_fails():
    schema = pa.DataFrameSchema(
        {
            'column1': pa.Column(int),
            'column2': pa.Column(float, pa.Check(lambda s: s < -1.2)),
            # you can provide a list of validators
            'column3': pa.Column(str, [
                pa.Check(lambda s: s.str.startswith('value')),
                pa.Check(lambda s: s.str.split('_', expand=True).shape[1] == 2)
            ]),
        },
        index=pa.Index(int),
        strict=True,
    )

    df = pd.DataFrame({'column1': [5, 1, np.nan]})
    validator = pandera_validators.PanderaDataFrameValidator(schema=schema)
    validation_result = validator.validate(df)
    assert not validation_result.passes
    assert 'A total of 4 schema errors were found' in validation_result.message  # TODO -- ensure this will stay constant with the contract


def test_basic_pandera_decorator_dataframe_passes():
    schema = pa.DataFrameSchema(
        {
            'column1': pa.Column(int),
            'column2': pa.Column(float, pa.Check(lambda s: s < -1.2)),
            # you can provide a list of validators
            'column3': pa.Column(str, [
                pa.Check(lambda s: s.str.startswith('value')),
                pa.Check(lambda s: s.str.split('_', expand=True).shape[1] == 2)
            ]),
        },
        index=pa.Index(int),
        strict=True,
    )

    df = pd.DataFrame({'column1': [5, 1, 0], 'column2': [-2.0, -1.9, -20.0], 'column3': ['value_0', 'value_1', 'value_2']})
    validator = pandera_validators.PanderaDataFrameValidator(schema=schema)
    validation_result = validator.validate(df)
    assert validation_result.passes


def test_basic_pandera_decorator_series_fails():
    schema = pa.SeriesSchema(
        str,
        checks=[
            pa.Check(lambda s: s.str.startswith('foo')),
            pa.Check(lambda s: s.str.endswith('bar')),
            pa.Check(lambda x: len(x) > 3, element_wise=True)
        ],
        nullable=False,
    )
    series = pd.Series(['foobar', 'foobox', 'foobaz'])
    validator = pandera_validators.PanderaSeriesSchemaValidator(schema=schema)
    validation_result = validator.validate(series)
    assert not validation_result.passes
    assert 'A total of 1 schema error' in validation_result.message


def test_basic_pandera_decorator_series_passes():
    schema = pa.SeriesSchema(
        str,
        checks=[
            pa.Check(lambda s: s.str.startswith('foo')),
            pa.Check(lambda s: s.str.endswith('bar')),
            pa.Check(lambda x: len(x) > 3, element_wise=True)
        ],
        nullable=False,
    )
    series = pd.Series(['foobar', 'foo_bar', 'foo__bar'])
    validator = pandera_validators.PanderaSeriesSchemaValidator(schema=schema)
    validation_result = validator.validate(series)
    assert validation_result.passes
