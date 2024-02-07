from typing import Callable

import pytest

from hamilton import graph_utils


@pytest.fixture()
def callable_a():
    """Default function implementation"""
    def A(external_input: int) -> int:
        return external_input % 7
    
    return A
    
@pytest.fixture()
def callable_a_body():
    """The function A() has modulo 5 instead of 7"""
    def A(external_input: int) -> int:
        return external_input % 5
    
    return A
    
    
@pytest.fixture()
def callable_a_docstring():
    """The function A() has a docstring"""
    def A(external_input: int) -> int:
        """This one has a docstring"""
        return external_input % 7
    
    return A


def test_different_hash_function_body(callable_a: Callable, callable_a_body: Callable):
    """Gives different hash for different function body"""
    assert (
        graph_utils.hash_callable(callable_a)
        != graph_utils.hash_callable(callable_a_body)
    )
    

def test_different_hash_docstring(callable_a: Callable, callable_a_docstring: Callable):
    """Sames different hash for different docstring"""
    assert (
        graph_utils.hash_callable(callable_a)
        != graph_utils.hash_callable(callable_a_docstring)
    )