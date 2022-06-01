from typing import Any, Type

import numpy
import numpy as np
import pandas as pd
import pytest

from hamilton import node
from hamilton.data_quality import default_validators
from hamilton.data_quality.base import DataValidator
from hamilton.data_quality.default_validators import resolve_default_validators, BaseDefaultValidator
from hamilton.function_modifiers import check_output
from hamilton.node import DependencyType

from resources.dq_dummy_examples import DUMMY_VALIDATORS_FOR_TESTING, SampleDataValidator2, SampleDataValidator1, SampleDataValidator3


@pytest.mark.parametrize('level', ['warn', 'fail'])
def test_validate_importance_level(level):
    DataValidator.validate_importance_level(level)


@pytest.mark.parametrize(
    'output_type, kwargs, importance, expected',
    [
        (int, {'equal_to': 1}, 'warn', [SampleDataValidator1(importance='warn', equal_to=1)]),
        (int, {'equal_to': 5}, 'fail', [SampleDataValidator1(importance='fail', equal_to=5)]),
        (pd.Series, {'dataset_length': 1}, 'warn', [SampleDataValidator2(importance='warn', dataset_length=1)]),
        (pd.Series, {'dataset_length': 5}, 'fail', [SampleDataValidator2(importance='fail', dataset_length=5)]),
        (
                pd.Series,
                {'dataset_length': 1, 'dtype': np.int64},
                'warn',
                [
                    SampleDataValidator2(importance='warn', dataset_length=1),
                    SampleDataValidator3(importance='warn', dtype=np.int64)
                ]
        ),
    ],
)
def test_resolve_default_validators(output_type, kwargs, importance, expected):
    resolved_validators = resolve_default_validators(
        output_type=output_type,
        importance=importance,
        available_validators=DUMMY_VALIDATORS_FOR_TESTING,
        **kwargs
    )
    assert resolved_validators == expected


@pytest.mark.parametrize(
    'output_type, kwargs, importance',
    [
        (str, {'dataset_length': 1}, 'warn'),
        (pd.Series, {'equal_to': 1}, 'warn')
    ],
)
def test_resolve_default_validators_error(output_type, kwargs, importance):
    with pytest.raises(ValueError):
        resolve_default_validators(
            output_type=output_type,
            importance=importance,
            available_validators=DUMMY_VALIDATORS_FOR_TESTING,
            **kwargs)


@pytest.mark.parametrize(
    'cls,param,data,should_pass',
    [
        (default_validators.DataInRangeValidatorPandas, (0, 1), pd.Series([0.1, 0.2, 0.3]), True),
        (default_validators.DataInRangeValidatorPandas, (0, 1), pd.Series([-30.0, 0.1, 0.2, 0.3, 100.0]), False),

        (default_validators.DataInRangeValidatorPrimitives, (0, 1), .5, True),
        (default_validators.DataInRangeValidatorPrimitives, (0, 1), 100.3, False),

        (default_validators.MaxFractionNansValidatorPandas, .5, pd.Series([1.0, 2.0, 3.0, None]), True),
        (default_validators.MaxFractionNansValidatorPandas, 0, pd.Series([1.0, 2.0, 3.0, None]), False),
        (default_validators.MaxFractionNansValidatorPandas, .5, pd.Series([1.0, 2.0, None, None]), True),
        (default_validators.MaxFractionNansValidatorPandas, .5, pd.Series([1.0, None, None, None]), False),
        (default_validators.MaxFractionNansValidatorPandas, .5, pd.Series([None, None, None, None]), False),

        (default_validators.DataTypeValidatorPandas, numpy.dtype('int'), pd.Series([1, 2, 3]), True),
        (default_validators.DataTypeValidatorPandas, numpy.dtype('int'), pd.Series([1.0, 2.0, 3.0]), False),
        (default_validators.DataTypeValidatorPandas, numpy.dtype('object'), pd.Series(['hello', 'goodbye']), True),
        (default_validators.DataTypeValidatorPandas, numpy.dtype('object'), pd.Series([1, 2]), False),

        (default_validators.PandasMaxStandardDevValidator, 1.0, pd.Series([.1, .2, .3, .4]), True),
        (default_validators.PandasMaxStandardDevValidator, 0.01, pd.Series([.1, .2, .3, .4]), False)
    ]
)
def test_default_data_validators(cls: Type[default_validators.BaseDefaultValidator], param: Any, data: Any, should_pass: bool):
    validator = cls(**{cls.arg(): param, "importance": "warn"})
    result = validator.validate(data)
    assert result.passes == should_pass
