import pytest

from hamilton import function_modifiers, node


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
