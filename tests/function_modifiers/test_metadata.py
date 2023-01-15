from typing import Dict, List, TypedDict

import pandas as pd
import pytest

from hamilton import function_modifiers, node
from hamilton.function_modifiers.base import InvalidDecoratorException
from hamilton.function_modifiers.metadata import typed_tags


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


def test_tag_outputs():
    @function_modifiers.extract_columns("a", "b")
    def dummy_tagged_function() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame.from_records({"a": [1], "b": [2]})

    annotation = function_modifiers.tag_outputs(
        a={"tag_a_gets": "tag_value_a_gets"},
        b={"tag_b_gets": "tag_value_b_gets"},
    )
    nodes = annotation.transform_dag(
        function_modifiers.base.resolve_nodes(dummy_tagged_function, {}),
        config={},
        fn=dummy_tagged_function,
    )
    node_map = {node_.name: node_ for node_ in nodes}
    assert node_map["a"].tags["tag_a_gets"] == "tag_value_a_gets"
    assert node_map["b"].tags["tag_b_gets"] == "tag_value_b_gets"


def test_tag_outputs_and_tag_together():
    """Tests that tag_outputs and tag work together"""

    @function_modifiers.tag(tag_key_everyone_gets="tag_value_everyone_gets")
    @function_modifiers.tag_outputs(
        a={"tag_a_gets": "tag_value_a_gets"},
        b={"tag_b_gets": "tag_value_b_gets"},
    )
    @function_modifiers.extract_columns("a", "b")
    def dummy_tagged_function() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame.from_records({"a": [1], "b": [2]})

    nodes = function_modifiers.base.resolve_nodes(dummy_tagged_function, {})
    node_map = {node_.name: node_ for node_ in nodes}
    assert node_map["a"].tags["tag_a_gets"] == "tag_value_a_gets"
    assert node_map["b"].tags["tag_b_gets"] == "tag_value_b_gets"
    assert node_map["a"].tags["tag_key_everyone_gets"] == "tag_value_everyone_gets"
    assert node_map["b"].tags["tag_key_everyone_gets"] == "tag_value_everyone_gets"


def test_tag_outputs_with_overrides():
    """Tests that tag_outputs and tag work together, where tag_outputs() override tag().
    Note this only works when tag_outputs() comes first. Otherwise this is undefined behavior
    (although it'll likely work in precedence order)"""

    @function_modifiers.tag_outputs(
        a={"tag_a_gets": "tag_value_a_gets", "tag_key_everyone_gets": "tag_value_just_a_gets"},
        b={"tag_b_gets": "tag_value_b_gets"},
    )
    @function_modifiers.tag(tag_key_everyone_gets="tag_value_everyone_gets")
    @function_modifiers.extract_columns("a", "b")
    def dummy_tagged_function() -> pd.DataFrame:
        """dummy doc"""
        return pd.DataFrame.from_records({"a": [1], "b": [2]})

    nodes = function_modifiers.base.resolve_nodes(dummy_tagged_function, {})
    node_map = {node_.name: node_ for node_ in nodes}
    assert node_map["a"].tags["tag_a_gets"] == "tag_value_a_gets"
    assert node_map["b"].tags["tag_b_gets"] == "tag_value_b_gets"
    assert node_map["a"].tags["tag_key_everyone_gets"] == "tag_value_just_a_gets"
    assert node_map["b"].tags["tag_key_everyone_gets"] == "tag_value_everyone_gets"


def test_typed_tags_success():
    """Tests the typed_tags decorator to ensure that it works in the basic case"""

    class FooType(TypedDict):
        foo: str
        bar: int

    foo = typed_tags(FooType)

    def dummy_tagged_function() -> int:
        """dummy doc"""
        return 1

    node_ = foo(foo="foo", bar=1).decorate_node(node.Node.from_fn(dummy_tagged_function))
    assert node_.tags["foo"] == "foo"
    assert node_.tags["bar"] == 1


def test_typed_tags_wrong_type_failure():
    """Tests the typed_tags decorator to ensure that it breaks when the wrong types are passed"""

    class FooType(TypedDict):
        foo: str
        bar: int

    foo = typed_tags(FooType)

    with pytest.raises(InvalidDecoratorException):

        @foo(foo=1, bar="bar")
        def dummy_tagged_function() -> int:
            """dummy doc"""
            return 1


def test_typed_tags_illegal_types_failure():
    """Tests the typed_tags decorator to ensure that it breaks when illegal types are declared"""

    class FooType(TypedDict):
        foo: Dict[str, dict]
        bar: List[List[int]]

    foo = typed_tags(FooType)

    with pytest.raises(InvalidDecoratorException):

        @foo(foo=1, bar="bar")
        def dummy_tagged_function() -> int:
            """dummy doc"""
            return 1


def test_layered_tags_success():
    """Tests to ensure that layered tags are applied appropriately"""

    class FooType(TypedDict):
        foo: str
        bar: int

    class BarType(TypedDict):
        bat: int
        baz: List[int]

    foo = typed_tags(FooType)
    bar = typed_tags(BarType)

    @foo(foo="foo", bar=1)
    @bar(bat=2, baz=[1, 2, 3])
    def dummy_tagged_function() -> int:
        """dummy doc"""
        return 1

    (node_,) = function_modifiers.base.resolve_nodes(dummy_tagged_function, {})
    assert node_.tags["foo"] == "foo"
    assert node_.tags["bar"] == 1
    assert node_.tags["bat"] == 2
    assert node_.tags["baz"] == [1, 2, 3]
