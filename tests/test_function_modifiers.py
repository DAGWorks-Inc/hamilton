import inspect
from typing import Any, Dict, List, Set

import numpy as np
import pandas as pd
import pytest

import hamilton.function_modifiers.function_modifiers_base
from hamilton import function_modifiers, function_modifiers_base, models, node
from hamilton.data_quality.base import DataValidationError, ValidationResult
from hamilton.function_modifiers import (
    DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG,
    IS_DATA_VALIDATOR_TAG,
    check_output,
    check_output_custom,
    does,
    source,
    value,
)
from hamilton.function_modifiers.dependencies import LiteralDependency, UpstreamDependency
from hamilton.function_modifiers.macros import ensure_function_empty
from hamilton.node import DependencyType
from tests.resources.dq_dummy_examples import (
    DUMMY_VALIDATORS_FOR_TESTING,
    SampleDataValidator2,
    SampleDataValidator3,
)


def test_parametrized_invalid_params():
    annotation = function_modifiers.parameterize_values(
        parameter="non_existant",
        assigned_output={("invalid_node_name", "invalid_doc"): "invalid_value"},
    )

    def no_param_node():
        pass

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(no_param_node)

    def wrong_param_node(valid_value):
        pass

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(wrong_param_node)


def test_parametrized_single_param_breaks_without_docs():
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        function_modifiers.parameterize_values(
            parameter="parameter", assigned_output={"only_node_name": "only_value"}
        )


def test_parametrized_single_param():
    annotation = function_modifiers.parameterize_values(
        parameter="parameter", assigned_output={("only_node_name", "only_doc"): "only_value"}
    )

    def identity(parameter: Any) -> Any:
        return parameter

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 1
    assert nodes[0].name == "only_node_name"
    assert nodes[0].type == Any
    assert nodes[0].documentation == "only_doc"
    called = nodes[0].callable()
    assert called == "only_value"


def test_parametrized_single_param_expanded():
    annotation = function_modifiers.parameterize_values(
        parameter="parameter",
        assigned_output={("node_name_1", "doc1"): "value_1", ("node_value_2", "doc2"): "value_2"},
    )

    def identity(parameter: Any) -> Any:
        return parameter

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    called_1 = nodes[0].callable()
    called_2 = nodes[1].callable()
    assert nodes[0].documentation == "doc1"
    assert nodes[1].documentation == "doc2"
    assert called_1 == "value_1"
    assert called_2 == "value_2"


def test_parametrized_with_multiple_params():
    annotation = function_modifiers.parameterize_values(
        parameter="parameter",
        assigned_output={("node_name_1", "doc1"): "value_1", ("node_value_2", "doc2"): "value_2"},
    )

    def identity(parameter: Any, static: Any) -> Any:
        return parameter, static

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    called_1 = nodes[0].callable(static="static_param")
    called_2 = nodes[1].callable(static="static_param")
    assert called_1 == ("value_1", "static_param")
    assert called_2 == ("value_2", "static_param")


def test_parametrized_input():
    annotation = function_modifiers.parametrized_input(
        parameter="parameter",
        variable_inputs={
            "input_1": ("test_1", "Function with first column as input"),
            "input_2": ("test_2", "Function with second column as input"),
        },
    )

    def identity(parameter: Any, static: Any) -> Any:
        return parameter, static

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    nodes = sorted(nodes, key=lambda n: n.name)
    assert [n.name for n in nodes] == ["test_1", "test_2"]
    assert set(nodes[0].input_types.keys()) == {"static", "input_1"}
    assert set(nodes[1].input_types.keys()) == {"static", "input_2"}


def test_parametrize_sources_validate_param_name():
    """Tests validate function of parameterize_sources capturing bad param name usage."""
    annotation = function_modifiers.parameterize_sources(
        parameterization={
            "test_1": dict(parameterfoo="input_1"),
        }
    )

    def identity(parameter1: str, parameter2: str, static: str) -> str:
        """Function with {parameter1} as first input"""
        return parameter1 + parameter2 + static

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(identity)


def test_parametrized_inputs_validate_reserved_param():
    """Tests validate function of parameterize_inputs catching reserved param usage."""
    annotation = function_modifiers.parameterize_sources(
        **{
            "test_1": dict(parameter2="input_1"),
        }
    )

    def identity(output_name: str, parameter2: str, static: str) -> str:
        """Function with {parameter2} as second input"""
        return output_name + parameter2 + static

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(identity)


def test_parametrized_inputs_validate_bad_doc_string():
    """Tests validate function of parameterize_inputs catching bad doc string."""
    annotation = function_modifiers.parameterize_sources(
        **{
            "test_1": dict(parameter2="input_1"),
        }
    )

    def identity(output_name: str, parameter2: str, static: str) -> str:
        """Function with {foo} as second input"""
        return output_name + parameter2 + static

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(identity)


def test_parametrized_inputs():
    annotation = function_modifiers.parameterize_sources(
        **{
            "test_1": dict(parameter1="input_1", parameter2="input_2"),
            "test_2": dict(parameter1="input_2", parameter2="input_1"),
        }
    )

    def identity(parameter1: str, parameter2: str, static: str) -> str:
        """Function with {parameter1} as first input"""
        return parameter1 + parameter2 + static

    nodes = annotation.expand_node(node.Node.from_fn(identity), {}, identity)
    assert len(nodes) == 2
    nodes = sorted(nodes, key=lambda n: n.name)
    assert [n.name for n in nodes] == ["test_1", "test_2"]
    assert set(nodes[0].input_types.keys()) == {"static", "input_1", "input_2"}
    assert nodes[0].documentation == "Function with input_1 as first input"
    assert set(nodes[1].input_types.keys()) == {"static", "input_1", "input_2"}
    assert nodes[1].documentation == "Function with input_2 as first input"
    result1 = nodes[0].callable(**{"input_1": "1", "input_2": "2", "static": "3"})
    assert result1 == "123"
    result2 = nodes[1].callable(**{"input_1": "1", "input_2": "2", "static": "3"})
    assert result2 == "213"


def test_invalid_column_extractor():
    annotation = function_modifiers.extract_columns("dummy_column")

    def no_param_node() -> int:
        pass

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(no_param_node)


def test_extract_columns_invalid_passing_list_to_column_extractor():
    """Ensures that people cannot pass in a list."""
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        function_modifiers.extract_columns(["a", "b", "c"])


def test_extract_columns_empty_args():
    """Tests that we fail on empty arguments."""
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        function_modifiers.extract_columns()


def test_extract_columns_happy():
    """Tests that we are happy with good arguments."""
    function_modifiers.extract_columns(*["a", ("b", "some doc"), "c"])


def test_valid_column_extractor():
    """Tests that things work, and that you can provide optional documentation."""
    annotation = function_modifiers.extract_columns("col_1", ("col_2", "col2_doc"))

    def dummy_df_generator() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    nodes = list(
        annotation.expand_node(node.Node.from_fn(dummy_df_generator), {}, dummy_df_generator)
    )
    assert len(nodes) == 3
    assert nodes[0] == node.Node(
        name=dummy_df_generator.__name__,
        typ=pd.DataFrame,
        doc_string=dummy_df_generator.__doc__,
        callabl=dummy_df_generator,
        tags={"module": "tests.test_function_modifiers"},
    )
    assert nodes[1].name == "col_1"
    assert nodes[1].type == pd.Series
    assert nodes[1].documentation == "dummy doc"  # we default to base function doc.
    assert nodes[1].input_types == {
        dummy_df_generator.__name__: (pd.DataFrame, DependencyType.REQUIRED)
    }
    assert nodes[2].name == "col_2"
    assert nodes[2].type == pd.Series
    assert nodes[2].documentation == "col2_doc"
    assert nodes[2].input_types == {
        dummy_df_generator.__name__: (pd.DataFrame, DependencyType.REQUIRED)
    }


def test_column_extractor_fill_with():
    def dummy_df() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    annotation = function_modifiers.extract_columns("col_3", fill_with=0)
    original_node, extracted_column_node = annotation.expand_node(
        node.Node.from_fn(dummy_df), {}, dummy_df
    )
    original_df = original_node.callable()
    extracted_column = extracted_column_node.callable(dummy_df=original_df)
    pd.testing.assert_series_equal(extracted_column, pd.Series([0, 0, 0, 0]), check_names=False)
    pd.testing.assert_series_equal(
        original_df["col_3"], pd.Series([0, 0, 0, 0]), check_names=False
    )  # it has to be in there now


def test_column_extractor_no_fill_with():
    def dummy_df_generator() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame({"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14]})

    annotation = function_modifiers.extract_columns("col_3")
    nodes = list(
        annotation.expand_node(node.Node.from_fn(dummy_df_generator), {}, dummy_df_generator)
    )
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        nodes[1].callable(dummy_df_generator=dummy_df_generator())


def test_no_code_validator():
    def no_code():
        pass

    def no_code_with_docstring():
        """This should still show up as having no code, even though it has a docstring"""
        pass

    def yes_code():
        """This should show up as having no code"""
        a = 0
        return a

    ensure_function_empty(no_code)
    ensure_function_empty(no_code_with_docstring)
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        ensure_function_empty(yes_code)


# Functions for  @does -- these are the functions we're "replacing"
def _no_params() -> int:
    pass


def _one_param(a: int) -> int:
    pass


def _two_params(a: int, b: int) -> int:
    pass


def _three_params(a: int, b: int, c: int) -> int:
    pass


def _three_params_with_defaults(a: int, b: int = 1, c: int = 2) -> int:
    pass


# functions we can/can't replace them with
def _empty() -> int:
    return 1


def _kwargs(**kwargs: int) -> int:
    return sum(kwargs.values())


def _kwargs_with_a(a: int, **kwargs: int) -> int:
    return a + sum(kwargs.values())


def _just_a(a: int) -> int:
    return a


def _just_b(b: int) -> int:
    return b


def _a_b_c(a: int, b: int, c: int) -> int:
    return a + b + c


@pytest.mark.parametrize(
    "fn,replace_with,argument_mapping,matches",
    [
        (_no_params, _empty, {}, True),
        (_no_params, _kwargs, {}, True),
        (_no_params, _kwargs_with_a, {}, False),
        (_no_params, _just_a, {}, False),
        (_no_params, _a_b_c, {}, False),
        (_one_param, _empty, {}, False),
        (_one_param, _kwargs, {}, True),
        (_one_param, _kwargs_with_a, {}, True),
        (_one_param, _just_a, {}, True),
        (_one_param, _just_b, {}, False),
        (_one_param, _just_b, {"b": "a"}, True),  # Replacing a with b makes the signatures match
        (_one_param, _just_b, {"c": "a"}, False),  # Replacing a with b makes the signatures match
        (_two_params, _empty, {}, False),
        (_two_params, _kwargs, {}, True),
        (_two_params, _kwargs_with_a, {}, True),  # b gets fed to kwargs
        (_two_params, _kwargs_with_a, {"foo": "b"}, True),  # Any kwargs work
        (_two_params, _kwargs_with_a, {"bar": "a"}, False),  # No param bar
        (_two_params, _just_a, {}, False),
        (_two_params, _just_b, {}, False),
        (_three_params, _a_b_c, {}, True),
        (_three_params, _a_b_c, {"d": "a"}, False),
        (_three_params, _a_b_c, {}, True),
        (_three_params, _a_b_c, {"a": "b", "b": "a"}, True),  # Weird case but why not?
        (_three_params, _kwargs_with_a, {}, True),
        (_three_params_with_defaults, _a_b_c, {}, True),
        (_three_params_with_defaults, _a_b_c, {"d": "a"}, False),
        (_three_params_with_defaults, _a_b_c, {}, True),
    ],
)
def test_ensure_function_signatures_compatible(fn, replace_with, argument_mapping, matches):
    assert (
        does.test_function_signatures_compatible(
            inspect.signature(fn), inspect.signature(replace_with), argument_mapping
        )
        == matches
    )


def test_does_function_modifier():
    def sum_(**kwargs: int) -> int:
        return sum(kwargs.values())

    def to_modify(param1: int, param2: int) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(sum_)
    node = annotation.generate_node(to_modify, {})
    assert node.name == "to_modify"
    assert node.callable(param1=1, param2=1) == 2
    assert node.documentation == to_modify.__doc__


def test_does_function_modifier_complex_types():
    def setify(**kwargs: List[int]) -> Set[int]:
        return set(sum(kwargs.values(), []))

    def to_modify(param1: List[int], param2: List[int]) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(setify)
    node = annotation.generate_node(to_modify, {})
    assert node.name == "to_modify"
    assert node.callable(param1=[1, 2, 3], param2=[4, 5, 6]) == {1, 2, 3, 4, 5, 6}
    assert node.documentation == to_modify.__doc__


def test_does_function_modifier_optionals():
    def sum_(param0: int, **kwargs: int) -> int:
        return sum(kwargs.values())

    def to_modify(param0: int, param1: int = 1, param2: int = 2) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(sum_)
    node_ = annotation.generate_node(to_modify, {})
    assert node_.name == "to_modify"
    assert node_.input_types["param0"][1] == DependencyType.REQUIRED
    assert node_.input_types["param1"][1] == DependencyType.OPTIONAL
    assert node_.input_types["param2"][1] == DependencyType.OPTIONAL
    assert node_.callable(param0=0) == 3
    assert node_.callable(param0=0, param1=0, param2=0) == 0
    assert node_.documentation == to_modify.__doc__


def test_does_with_argument_mapping():
    def _sum_multiply(param0: int, param1: int, param2: int) -> int:
        return param0 + param1 * param2

    def to_modify(parama: int, paramb: int = 1, paramc: int = 2) -> int:
        """This sums the inputs it gets..."""
        pass

    annotation = does(_sum_multiply, param0="parama", param1="paramb", param2="paramc")
    node = annotation.generate_node(to_modify, {})
    assert node.name == "to_modify"
    assert node.input_types["parama"][1] == DependencyType.REQUIRED
    assert node.input_types["paramb"][1] == DependencyType.OPTIONAL
    assert node.input_types["paramc"][1] == DependencyType.OPTIONAL
    assert node.callable(parama=0) == 2
    assert node.callable(parama=0, paramb=1, paramc=2) == 2
    assert node.callable(parama=1, paramb=4) == 9
    assert node.documentation == to_modify.__doc__


def test_model_modifier():
    config = {
        "my_column_model_params": {
            "col_1": 0.5,
            "col_2": 0.5,
        }
    }

    class LinearCombination(models.BaseModel):
        def get_dependents(self) -> List[str]:
            return list(self.config_parameters.keys())

        def predict(self, **columns: pd.Series) -> pd.Series:
            return sum(
                self.config_parameters[column_name] * column
                for column_name, column in columns.items()
            )

    def my_column() -> pd.Series:
        """Column that will be annotated by a model"""
        pass

    annotation = function_modifiers.model(LinearCombination, "my_column_model_params")
    annotation.validate(my_column)
    model_node = annotation.generate_node(my_column, config)
    assert model_node.input_types["col_1"][0] == model_node.input_types["col_2"][0] == pd.Series
    assert model_node.type == pd.Series
    pd.testing.assert_series_equal(
        model_node.callable(col_1=pd.Series([1]), col_2=pd.Series([2])), pd.Series([1.5])
    )

    def bad_model(col_1: pd.Series, col_2: pd.Series) -> pd.Series:
        return col_1 * 0.5 + col_2 * 0.5

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(bad_model)


def test_sanitize_function_name():
    assert function_modifiers_base.sanitize_function_name("fn_name__v2") == "fn_name"
    assert function_modifiers_base.sanitize_function_name("fn_name") == "fn_name"


def test_config_modifier_validate():
    def valid_fn() -> int:
        pass

    def valid_fn__this_is_also_valid() -> int:
        pass

    function_modifiers.config.when(key="value").validate(valid_fn__this_is_also_valid)
    function_modifiers.config.when(key="value").validate(valid_fn)

    def invalid_function__() -> int:
        pass

    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        function_modifiers.config.when(key="value").validate(invalid_function__)


def test_config_when():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key="value")
    assert annotation.resolve(config_when_fn, {"key": "value"}) is not None
    assert annotation.resolve(config_when_fn, {"key": "wrong_value"}) is None


def test_config_when_not():
    def config_when_not_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not(key="value")
    assert annotation.resolve(config_when_not_fn, {"key": "other_value"}) is not None
    assert annotation.resolve(config_when_not_fn, {"key": "value"}) is None


def test_config_when_in():
    def config_when_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_in(key=["valid_value", "another_valid_value"])
    assert annotation.resolve(config_when_in_fn, {"key": "valid_value"}) is not None
    assert annotation.resolve(config_when_in_fn, {"key": "another_valid_value"}) is not None
    assert annotation.resolve(config_when_in_fn, {"key": "not_a_valid_value"}) is None


def test_config_when_not_in():
    def config_when_not_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not_in(
        key=["invalid_value", "another_invalid_value"]
    )
    assert annotation.resolve(config_when_not_in_fn, {"key": "invalid_value"}) is None
    assert annotation.resolve(config_when_not_in_fn, {"key": "another_invalid_value"}) is None
    assert annotation.resolve(config_when_not_in_fn, {"key": "valid_value"}) is not None


def test_config_name_resolution():
    def fn__v2() -> int:
        pass

    annotation = function_modifiers.config.when(key="value")
    assert annotation.resolve(fn__v2, {"key": "value"}).__name__ == "fn"


def test_config_when_with_custom_name():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key="value", name="new_function_name")
    assert annotation.resolve(config_when_fn, {"key": "value"}).__name__ == "new_function_name"


@pytest.mark.parametrize(
    "fields",
    [
        (None),  # empty
        ("string_input"),  # not a dict
        (["string_input"]),  # not a dict
        ({}),  # empty dict
        ({1: "string", "field": str}),  # invalid dict
        ({"field": lambda x: x, "field2": int}),  # invalid dict
    ],
)
def test_extract_fields_constructor_errors(fields):
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        function_modifiers.extract_fields(fields)


@pytest.mark.parametrize(
    "fields",
    [
        ({"field": np.ndarray, "field2": str}),
        ({"field": dict, "field2": int, "field3": list, "field4": float, "field5": str}),
    ],
)
def test_extract_fields_constructor_happy(fields):
    """Tests that we are happy with good arguments."""
    function_modifiers.extract_fields(fields)


@pytest.mark.parametrize(
    "return_type",
    [
        (dict),
        (Dict),
        (Dict[str, str]),
        (Dict[str, Any]),
    ],
)
def test_extract_fields_validate_happy(return_type):
    def return_dict() -> return_type:
        return {}

    annotation = function_modifiers.extract_fields({"test": int})
    annotation.validate(return_dict)


@pytest.mark.parametrize("return_type", [(int), (list), (np.ndarray), (pd.DataFrame)])
def test_extract_fields_validate_errors(return_type):
    def return_dict() -> return_type:
        return {}

    annotation = function_modifiers.extract_fields({"test": int})
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        annotation.validate(return_dict)


def test_valid_extract_fields():
    """Tests whole extract_fields decorator."""
    annotation = function_modifiers.extract_fields(
        {"col_1": list, "col_2": int, "col_3": np.ndarray}
    )

    def dummy_dict_generator() -> dict:
        """dummy doc"""
        return {"col_1": [1, 2, 3, 4], "col_2": 1, "col_3": np.ndarray([1, 2, 3, 4])}

    nodes = list(
        annotation.expand_node(node.Node.from_fn(dummy_dict_generator), {}, dummy_dict_generator)
    )
    assert len(nodes) == 4
    assert nodes[0] == node.Node(
        name=dummy_dict_generator.__name__,
        typ=dict,
        doc_string=dummy_dict_generator.__doc__,
        callabl=dummy_dict_generator,
        tags={"module": "tests.test_function_modifiers"},
    )
    assert nodes[1].name == "col_1"
    assert nodes[1].type == list
    assert nodes[1].documentation == "dummy doc"  # we default to base function doc.
    assert nodes[1].input_types == {dummy_dict_generator.__name__: (dict, DependencyType.REQUIRED)}
    assert nodes[2].name == "col_2"
    assert nodes[2].type == int
    assert nodes[2].documentation == "dummy doc"
    assert nodes[2].input_types == {dummy_dict_generator.__name__: (dict, DependencyType.REQUIRED)}
    assert nodes[3].name == "col_3"
    assert nodes[3].type == np.ndarray
    assert nodes[3].documentation == "dummy doc"
    assert nodes[3].input_types == {dummy_dict_generator.__name__: (dict, DependencyType.REQUIRED)}


def test_extract_fields_fill_with():
    def dummy_dict() -> dict:
        """dummy doc"""
        return {"col_1": [1, 2, 3, 4], "col_2": 1, "col_3": np.ndarray([1, 2, 3, 4])}

    annotation = function_modifiers.extract_fields({"col_2": int, "col_4": float}, fill_with=1.0)
    original_node, extracted_field_node, missing_field_node = annotation.expand_node(
        node.Node.from_fn(dummy_dict), {}, dummy_dict
    )
    original_dict = original_node.callable()
    extracted_field = extracted_field_node.callable(dummy_dict=original_dict)
    missing_field = missing_field_node.callable(dummy_dict=original_dict)
    assert extracted_field == 1
    assert missing_field == 1.0


def test_extract_fields_no_fill_with():
    def dummy_dict() -> dict:
        """dummy doc"""
        return {"col_1": [1, 2, 3, 4], "col_2": 1, "col_3": np.ndarray([1, 2, 3, 4])}

    annotation = function_modifiers.extract_fields({"col_4": int})
    nodes = list(annotation.expand_node(node.Node.from_fn(dummy_dict), {}, dummy_dict))
    with pytest.raises(
        hamilton.function_modifiers.function_modifiers_base.InvalidDecoratorException
    ):
        nodes[1].callable(dummy_dict=dummy_dict())


def test_tags():
    def dummy_tagged_function() -> int:
        """dummy doc"""
        return 1

    annotation = function_modifiers.tag(foo="bar", bar="baz")
    node_ = annotation.decorate_node(node.Node.from_fn(dummy_tagged_function))
    assert "foo" in node_.tags
    assert "bar" in node_.tags


@pytest.mark.parametrize(
    "key",
    [
        "hamilton",  # Reserved key
        "foo@",  # Invalid identifier
        "foo bar",  # No spaces
        "foo.bar+baz",  # Invalid key, not a valid identifier
        "" "...",  # Empty not allowed  # Empty elements not allowed
    ],
)
def test_tags_invalid_key(key):
    assert not function_modifiers.tag._key_allowed(key)


@pytest.mark.parametrize(
    "key",
    [
        "bar.foo",
        "foo",  # Invalid identifier
        "foo.bar.baz",  # Invalid key, not a valid identifier
    ],
)
def test_tags_valid_key(key):
    assert function_modifiers.tag._key_allowed(key)


@pytest.mark.parametrize("value", [None, False, [], ["foo", "bar"]])
def test_tags_invalid_value(value):
    assert not function_modifiers.tag._value_allowed(value)


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


def concat(upstream_parameter: str, literal_parameter: str) -> Any:
    """Concatenates {upstream_parameter} with literal_parameter"""
    return f"{upstream_parameter}{literal_parameter}"


def test_parametrized_full_no_replacement():
    annotation = function_modifiers.parameterize(replace_no_parameters={})
    (node_,) = annotation.expand_node(node.Node.from_fn(concat), {}, concat)
    assert node_.callable(upstream_parameter="foo", literal_parameter="bar") == "foobar"
    assert node_.input_types == {
        "literal_parameter": (str, DependencyType.REQUIRED),
        "upstream_parameter": (str, DependencyType.REQUIRED),
    }
    assert node_.documentation == concat.__doc__.format(upstream_parameter="upstream_parameter")


def test_parametrized_full_replace_just_upstream():
    annotation = function_modifiers.parameterize(
        replace_just_upstream_parameter={"upstream_parameter": source("foo_source")},
    )
    (node_,) = annotation.expand_node(node.Node.from_fn(concat), {}, concat)
    assert node_.input_types == {
        "literal_parameter": (str, DependencyType.REQUIRED),
        "foo_source": (str, DependencyType.REQUIRED),
    }
    assert node_.callable(foo_source="foo", literal_parameter="bar") == "foobar"
    assert node_.documentation == concat.__doc__.format(upstream_parameter="foo_source")


def test_parametrized_full_replace_just_literal():
    annotation = function_modifiers.parameterize(
        replace_just_literal_parameter={"literal_parameter": value("bar")}
    )
    (node_,) = annotation.expand_node(node.Node.from_fn(concat), {}, concat)
    assert node_.input_types == {"upstream_parameter": (str, DependencyType.REQUIRED)}
    assert node_.callable(upstream_parameter="foo") == "foobar"
    assert node_.documentation == concat.__doc__.format(upstream_parameter="upstream_parameter")


def test_parametrized_full_replace_both():
    annotation = function_modifiers.parameterize(
        replace_both_parameters={
            "upstream_parameter": source("foo_source"),
            "literal_parameter": value("bar"),
        }
    )
    (node_,) = annotation.expand_node(node.Node.from_fn(concat), {}, concat)
    assert node_.input_types == {"foo_source": (str, DependencyType.REQUIRED)}
    assert node_.callable(foo_source="foo") == "foobar"
    assert node_.documentation == concat.__doc__.format(upstream_parameter="foo_source")


def test_parametrized_full_multiple_replacements():
    args = dict(
        replace_no_parameters=({}, "fn with no parameters replaced"),
        replace_just_upstream_parameter=(
            {"upstream_parameter": source("foo_source")},
            "fn with upstream_parameter set to node foo",
        ),
        replace_just_literal_parameter=(
            {"literal_parameter": value("bar")},
            "fn with upstream_parameter set to node foo",
        ),
        replace_both_parameters=(
            {"upstream_parameter": source("foo_source"), "literal_parameter": value("bar")},
            "fn with both parameters replaced",
        ),
    )
    annotation = function_modifiers.parameterize(**args)
    nodes = annotation.expand_node(node.Node.from_fn(concat), {}, concat)
    assert len(nodes) == 4
    # test out that documentation is assigned correctly
    assert [node_.documentation for node_ in nodes] == [args[node_.name][1] for node_ in nodes]


@pytest.mark.parametrize(
    "upstream_source,expected",
    [("foo", UpstreamDependency("foo")), (UpstreamDependency("bar"), UpstreamDependency("bar"))],
)
def test_upstream(upstream_source, expected):
    assert source(upstream_source) == expected


@pytest.mark.parametrize(
    "literal_value,expected",
    [
        ("foo", LiteralDependency("foo")),
        (LiteralDependency("foo"), LiteralDependency("foo")),
        (1, LiteralDependency(1)),
    ],
)
def test_literal(literal_value, expected):
    assert value(literal_value) == expected
