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
