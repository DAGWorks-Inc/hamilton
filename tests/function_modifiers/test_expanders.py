from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

import hamilton.function_modifiers
from hamilton import function_modifiers, node
from hamilton.function_modifiers.dependencies import source, value
from hamilton.node import DependencyType


def test_parametrized_invalid_params():
    annotation = function_modifiers.parameterize_values(
        parameter="non_existant",
        assigned_output={("invalid_node_name", "invalid_doc"): "invalid_value"},
    )

    def no_param_node():
        pass

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        annotation.validate(no_param_node)

    def wrong_param_node(valid_value):
        pass

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        annotation.validate(wrong_param_node)


def test_parametrized_single_param_breaks_without_docs():
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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

    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        annotation.validate(no_param_node)


def test_extract_columns_invalid_passing_list_to_column_extractor():
    """Ensures that people cannot pass in a list."""
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        function_modifiers.extract_columns(["a", "b", "c"])


def test_extract_columns_empty_args():
    """Tests that we fail on empty arguments."""
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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
        tags={"module": "tests.function_modifiers.test_expanders"},
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
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        nodes[1].callable(dummy_df_generator=dummy_df_generator())


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
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
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
        tags={"module": "tests.function_modifiers.test_expanders"},
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
    with pytest.raises(hamilton.function_modifiers.base.InvalidDecoratorException):
        nodes[1].callable(dummy_dict=dummy_dict())


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


def test_parameterized_extract_columns():
    annotation = function_modifiers.parameterize_extract_columns(
        function_modifiers.ParameterizedExtract(
            ("outseries1a", "outseries2a"),
            {"input1": source("inseries1a"), "input2": source("inseries1b"), "input3": value(10)},
        ),
        function_modifiers.ParameterizedExtract(
            ("outseries1b", "outseries2b"),
            {"input1": source("inseries2a"), "input2": source("inseries2b"), "input3": value(100)},
        ),
    )

    def fn(input1: pd.Series, input2: pd.Series, input3: float) -> pd.DataFrame:
        return pd.concat([input1 * input2 * input3, input1 + input2 + input3], axis=1)

    nodes = annotation.expand_node(node.Node.from_fn(fn), {}, fn)
    # For each parameterized set, we have two outputs and the dataframe node
    assert len(nodes) == 6
    nodes_by_name = {node_.name: node_ for node_ in nodes}
    # Test that it produces the expected results
    pd.testing.assert_frame_equal(
        nodes_by_name["fn__0"](inseries1a=pd.Series([1]), inseries1b=pd.Series([1])),
        pd.DataFrame.from_dict({"outseries1a": [10], "outseries2a": [12]}),
    )
    pd.testing.assert_frame_equal(
        nodes_by_name["fn__1"](inseries2a=pd.Series([1]), inseries2b=pd.Series([1])),
        pd.DataFrame.from_dict({"outseries1b": [100], "outseries2b": [102]}),
    )
    # test that each of the "extractor" nodes produces exactly what we expect
    assert nodes_by_name["outseries1a"](fn__0=pd.DataFrame({"outseries1a": [10]}))[0] == 10
    assert nodes_by_name["outseries2a"](fn__0=pd.DataFrame({"outseries2a": [20]}))[0] == 20
    assert nodes_by_name["outseries1b"](fn__1=pd.DataFrame({"outseries1b": [30]}))[0] == 30
    assert nodes_by_name["outseries2b"](fn__1=pd.DataFrame({"outseries2b": [40]}))[0] == 40
