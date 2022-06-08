import pytest

import hamilton.driver
import tests.resources.data_quality
from hamilton.data_quality.base import ValidationResult, DataValidationError


def test_data_quality_workflow_passes():
    driver = hamilton.driver.Driver({}, tests.resources.data_quality)
    all_vars = driver.list_available_variables()
    result = driver.raw_execute([var.name for var in all_vars], inputs={'data_quality_should_fail': False})
    dq_nodes = [var.name for var in all_vars if var.tags.get('hamilton.data_quality.contains_dq_results', False)]
    assert len(dq_nodes) == 1
    dq_result = result[dq_nodes[0]]
    assert isinstance(dq_result, ValidationResult)
    assert dq_result.passes is True


def test_data_quality_workflow_fails():
    driver = hamilton.driver.Driver({}, tests.resources.data_quality)
    all_vars = driver.list_available_variables()
    with pytest.raises(DataValidationError):
        driver.raw_execute([var.name for var in all_vars], inputs={'data_quality_should_fail': True})
