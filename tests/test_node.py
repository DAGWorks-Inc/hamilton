import sys
from typing import Any, Literal, TypeVar

import numpy as np
import numpy.typing as npt
import pytest

from hamilton.node import DependencyType, Node


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


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires python 3.9 or higher")
def test_node_handles_annotated():
    from typing import Annotated

    DType = TypeVar("DType", bound=np.generic)
    ArrayN = Annotated[npt.NDArray[DType], Literal["N"]]

    def annotated_func(first: ArrayN[np.float64], other: float = 2.0) -> ArrayN[np.float64]:
        return first * other

    node = Node.from_fn(annotated_func)
    assert node.name == "annotated_func"
    expected = {
        "first": (
            Annotated[np.ndarray[Any, np.dtype[np.float64]], Literal["N"]],
            DependencyType.REQUIRED,
        ),
        "other": (float, DependencyType.OPTIONAL),
    }
    assert node.input_types == expected
    assert node.type == Annotated[np.ndarray[Any, np.dtype[np.float64]], Literal["N"]]
