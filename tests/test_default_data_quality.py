from typing import Any, Type

import pandas as pd
import pytest

from hamilton.data_quality.base import DataValidator
from hamilton.data_quality.default_validators import resolve_default_validators

from resources.dq_dummy_examples import DUMMY_VALIDATORS_FOR_TESTING, SampleDataValidator2, SampleDataValidator1


@pytest.mark.parametrize('level', ['warn', 'fail'])
def test_validate_importance_level(level):
    DataValidator.validate_importance_level(level)


@pytest.mark.parametrize(
    'output_type, kwargs, importance, expected',
    [
        (int, {'equal_to': 1}, 'warn', SampleDataValidator1(importance='warn', equal_to=1)),
        (int, {'equal_to': 5}, 'fail', SampleDataValidator1(importance='fail', equal_to=5)),
        (pd.Series, {'dataset_length': 1}, 'warn', SampleDataValidator2(importance='warn', dataset_length=1)),
        (pd.Series, {'dataset_length': 5}, 'fail', SampleDataValidator2(importance='fail', dataset_length=5))
    ],
)
def test_resolve_default_validators(output_type, kwargs, importance, expected):
    assert resolve_default_validators(
        output_type=output_type,
        importance=importance,
        available_validators=DUMMY_VALIDATORS_FOR_TESTING,
        **kwargs
    ) == expected


@pytest.mark.parametrize(
    'output_type, kwargs, importance',
    [
        (str, {'dataset_length': 1}, 'warn'),
        (pd.Series, {'equal_to': 1}, 'warn')
    ],
)
def test_resolve_default_validators(output_type, kwargs, importance):
    with pytest.raises(ValueError):
        resolve_default_validators(
            output_type=output_type,
            importance=importance,
            available_validators=DUMMY_VALIDATORS_FOR_TESTING,
            **kwargs
        )
