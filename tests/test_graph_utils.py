from typing import Callable

import pytest

from hamilton import graph_utils


@pytest.fixture()
def func_a():
    """Default function implementation"""

    def A(external_input: int) -> int:
        return external_input % 7

    return A


@pytest.fixture()
def func_a_body():
    """The function A() has modulo 5 instead of 7"""

    def A(external_input: int) -> int:
        return external_input % 5

    return A


@pytest.fixture()
def func_a_docstring():
    """The function A() has a docstring"""

    def A(external_input: int) -> int:
        """This one has a docstring"""
        return external_input % 7

    return A


@pytest.fixture()
def func_a_comment():
    """The function A() has a comment"""

    def A(external_input: int) -> int:
        """This one has a docstring"""
        return external_input % 7  # a comment

    return A


@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_function_body(func_a: Callable, func_a_body: Callable, strip: bool):
    """Gives different hash for different function body"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_body_hash = graph_utils.hash_source_code(func_a_body, strip=strip)
    assert func_a_hash != func_a_body_hash


@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_docstring(func_a: Callable, func_a_docstring: Callable, strip: bool):
    """Sames different hash for different docstring"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_docstring_hash = graph_utils.hash_source_code(func_a_docstring, strip=strip)
    assert (func_a_hash == func_a_docstring_hash) is strip


@pytest.mark.parametrize("strip", [True, False])
def test_different_hash_comment(func_a: Callable, func_a_comment: Callable, strip: bool):
    """Sames different hash for different docstring"""
    func_a_hash = graph_utils.hash_source_code(func_a, strip=strip)
    func_a_comment = graph_utils.hash_source_code(func_a_comment, strip=strip)
    assert (func_a_hash == func_a_comment) is strip
