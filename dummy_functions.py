"""
Module for dummy functions to test graph things with.
"""
# we import this to check we don't pull in this function when parsing this module.
from tests.resources import only_import_me


def A(b: int, c: int) -> int:
    """Function that should be come part of the graph"""
    return b + c


def _do_not_import_me(some_input: int, some_input2: int) -> int:
    """Function that should not become part of the graph."""
    only_import_me.this_is_not_something_we_should_import()
    return some_input + some_input2


def B(A: int) -> int:
    """Function that should be come part of the graph"""
    return A * A
