import pytest

from hamilton.node import Node


def test_node_from_fn_happy():
    def fn() -> int:
        return 1

    node = Node.from_fn(fn)
    assert node.name == "fn"
    assert node.callable() == 1
    assert node.input_types == {}
    assert node.type == int


def test_node_from_fn_sad_no_type_hint():
    def fn():
        return 1

    with pytest.raises(ValueError):
        Node.from_fn(fn)


def test_node_copy_with_retains_originating_functions():
    def fn() -> int:
        return 1

    node = Node.from_fn(fn)
    node_copy = node.copy_with(name="rename_fn", originating_functions=(fn,))
    assert node_copy.originating_functions == (fn,)
    assert node_copy.name == "rename_fn"
    node_copy_copy = node_copy.copy_with(name="rename_fn_again")
    assert node_copy_copy.originating_functions == (fn,)
    assert node_copy_copy.name == "rename_fn_again"
