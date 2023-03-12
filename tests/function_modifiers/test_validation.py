from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from hamilton import ad_hoc_utils, driver, node
from hamilton.data_quality.base import (
    DataValidationError,
    DataValidationLevel,
    DataValidator,
    ValidationResult,
)
from hamilton.function_modifiers import (
    DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG,
    IS_DATA_VALIDATOR_TAG,
    check_output,
    check_output_custom,
)
from hamilton.function_modifiers.validation import ValidatorConfig
from hamilton.node import DependencyType
from tests.resources.dq_dummy_examples import (
    DUMMY_VALIDATORS_FOR_TESTING,
    SampleDataValidator2,
    SampleDataValidator3,
)


def test_check_output_node_transform():
    decorator = check_output(
        importance="warn",
        default_decorator_candidates=DUMMY_VALIDATORS_FOR_TESTING,
        dataset_length=1,
        dtype=np.int64,
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    subdag = decorator.transform_node(node_, config={}, fn=fn)
    assert 4 == len(subdag)
    subdag_as_dict = {node_.name: node_ for node_ in subdag}
    assert sorted(subdag_as_dict.keys()) == [
        "fn",
        "fn_dummy_data_validator_2",
        "fn_dummy_data_validator_3",
        "fn_raw",
    ]
    # TODO -- change when we change the naming scheme
    assert subdag_as_dict["fn_raw"].input_types["input"][1] == DependencyType.REQUIRED
    assert 3 == len(
        subdag_as_dict["fn"].input_types
    )  # Three dependencies -- the two with DQ + the original
    # The final function should take in everything but only use the raw results
    assert (
        subdag_as_dict["fn"].callable(
            fn_raw="test",
            fn_dummy_data_validator_2=ValidationResult(True, "", {}),
            fn_dummy_data_validator_3=ValidationResult(True, "", {}),
        )
        == "test"
    )


def test_check_output_custom_node_transform():
    decorator = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="warn"),
        SampleDataValidator3(dtype=np.int64, importance="warn"),
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    subdag = decorator.transform_node(node_, config={}, fn=fn)
    assert 4 == len(subdag)
    subdag_as_dict = {node_.name: node_ for node_ in subdag}
    assert sorted(subdag_as_dict.keys()) == [
        "fn",
        "fn_dummy_data_validator_2",
        "fn_dummy_data_validator_3",
        "fn_raw",
    ]
    # TODO -- change when we change the naming scheme
    assert subdag_as_dict["fn_raw"].input_types["input"][1] == DependencyType.REQUIRED
    assert 3 == len(
        subdag_as_dict["fn"].input_types
    )  # Three dependencies -- the two with DQ + the original
    data_validators = [
        value
        for value in subdag_as_dict.values()
        if value.tags.get("hamilton.data_quality.contains_dq_results", False)
    ]
    assert len(data_validators) == 2  # One for each validator
    first_validator, _ = data_validators
    assert (
        IS_DATA_VALIDATOR_TAG in first_validator.tags
        and first_validator.tags[IS_DATA_VALIDATOR_TAG] is True
    )  # Validates that all the required tags are included
    assert (
        DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG in first_validator.tags
        and first_validator.tags[DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG] == "fn"
    )

    # The final function should take in everything but only use the raw results
    assert (
        subdag_as_dict["fn"].callable(
            fn_raw="test",
            fn_dummy_data_validator_2=ValidationResult(True, "", {}),
            fn_dummy_data_validator_3=ValidationResult(True, "", {}),
        )
        == "test"
    )


def test_check_output_custom_node_transform_raises_exception_with_failure():
    decorator = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
        SampleDataValidator3(dtype=np.int64, importance="fail"),
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    subdag = decorator.transform_node(node_, config={}, fn=fn)
    assert 4 == len(subdag)
    subdag_as_dict = {node_.name: node_ for node_ in subdag}

    with pytest.raises(DataValidationError):
        subdag_as_dict["fn"].callable(
            fn_raw=pd.Series([1.0, 2.0, 3.0]),
            fn_dummy_data_validator_2=ValidationResult(False, "", {}),
            fn_dummy_data_validator_3=ValidationResult(False, "", {}),
        )


def test_check_output_custom_node_transform_layered():
    decorator_1 = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="warn"),
    )

    decorator_2 = check_output_custom(SampleDataValidator3(dtype=np.int64, importance="warn"))

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    subdag_first_transformation = decorator_1.transform_dag([node_], config={}, fn=fn)
    subdag_second_transformation = decorator_2.transform_dag(
        subdag_first_transformation, config={}, fn=fn
    )
    # One node for each dummy validator
    # One final node
    # One intermediate node for each of the functions (E.G. raw)
    # TODO -- ensure that the intermediate nodes don't share names
    assert 5 == len(subdag_second_transformation)


def test_data_quality_constants_for_api_consistency():
    # simple tests to test data quality constants remain the same
    assert IS_DATA_VALIDATOR_TAG == "hamilton.data_quality.contains_dq_results"
    assert DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG == "hamilton.data_quality.source_node"


@pytest.mark.parametrize(
    "validator,config,node_name,expected_result",
    [
        (
            SampleDataValidator2(0, "warn"),
            {},
            "test",
            ValidatorConfig(True, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "fail"),
            {},
            "test",
            ValidatorConfig(True, DataValidationLevel.FAIL),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {"data_quality.test": {"enabled": False}},
            "test",
            ValidatorConfig(False, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {"data_quality.test": {"enabled": True}},
            "test",
            ValidatorConfig(True, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "fail"),
            {"data_quality.test": {"enabled": False, "importance": "warn"}},
            "test",
            ValidatorConfig(False, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {"data_quality.global": {"enabled": False}},
            "test",
            ValidatorConfig(False, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {"data_quality.global": {"enabled": False, "importance": "warn"}},
            "test",
            ValidatorConfig(False, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {
                "data_quality.global": {"enabled": False, "importance": "warn"},
                "data_quality.test": {"enabled": True},
            },
            "test",
            ValidatorConfig(True, DataValidationLevel.WARN),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {"data_quality.global": {"enabled": True}, "data_quality.test": {"importance": "fail"}},
            "test",
            ValidatorConfig(True, DataValidationLevel.FAIL),
        ),
        (
            SampleDataValidator2(0, "warn"),
            {
                "data_quality.global": {"enabled": False},
                "data_quality.test": {"enabled": True, "importance": "fail"},
            },
            "test",
            ValidatorConfig(True, DataValidationLevel.FAIL),
        ),
    ],
)
def test_validator_config_derive(
    validator: DataValidator,
    config: Dict[str, Any],
    node_name: str,
    expected_result: ValidatorConfig,
):
    assert ValidatorConfig.from_validator(validator, config, node_name) == expected_result


def test_validator_config_produces_no_validation_with_global():
    decorator = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
        SampleDataValidator3(dtype=np.int64, importance="fail"),
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    config = {"data_quality.global": {"enabled": False}}
    subdag = decorator.transform_node(node_, config=config, fn=fn)
    assert 1 == len(subdag)
    node_, *_ = subdag
    assert node_.name == "fn"
    # Ensure nothing's been messed with
    pd.testing.assert_series_equal(node_(pd.Series([1.0, 2.0, 3.0])), pd.Series([1.0, 2.0, 3.0]))


def test_validator_config_produces_no_validation_with_node_override():
    decorator = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
        SampleDataValidator3(dtype=np.int64, importance="fail"),
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    config = {f"data_quality.{node_.name}": {"enabled": False}}
    subdag = decorator.transform_node(node_, config=config, fn=fn)
    assert 1 == len(subdag)
    node_, *_ = subdag
    assert node_.name == "fn"
    # Ensure nothing's been messed with
    pd.testing.assert_series_equal(node_(pd.Series([1.0, 2.0, 3.0])), pd.Series([1.0, 2.0, 3.0]))


def test_validator_config_produces_no_validation_with_node_level_override():
    decorator = check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
        SampleDataValidator3(dtype=np.int64, importance="fail"),
    )

    def fn(input: pd.Series) -> pd.Series:
        return input

    node_ = node.Node.from_fn(fn)
    config = {f"data_quality.{node_.name}": {"importance": "warn"}}
    subdag = decorator.transform_node(node_, config=config, fn=fn)
    assert 4 == len(subdag)
    nodes_by_name = {n.name: n for n in subdag}
    # We set this to warn so this should not break
    nodes_by_name["fn"].callable(
        fn_raw=pd.Series([1.0, 2.0, 3.0]),
        fn_dummy_data_validator_2=ValidationResult(False, "", {}),
        fn_dummy_data_validator_3=ValidationResult(False, "", {}),
    )


def test_data_validator_end_to_end_fails():
    @check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
    )
    def data_quality_check_that_doesnt_pass() -> pd.Series:
        return pd.Series([1, 2])

    dr = driver.Driver(
        {}, ad_hoc_utils.create_temporary_module(data_quality_check_that_doesnt_pass)
    )
    with pytest.raises(DataValidationError):
        dr.execute(final_vars=["data_quality_check_that_doesnt_pass"], inputs={})


def test_data_validator_end_to_end_succeed_when_node_disabled():
    @check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="fail"),
    )
    def data_quality_check_that_doesnt_pass() -> pd.Series:
        return pd.Series([1, 2])

    dr = driver.Driver(
        {f"data_quality.{data_quality_check_that_doesnt_pass.__name__}": {"enabled": False}},
        ad_hoc_utils.create_temporary_module(data_quality_check_that_doesnt_pass),
    )
    dr.execute(final_vars=["data_quality_check_that_doesnt_pass"], inputs={})
