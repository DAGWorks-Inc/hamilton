"""
Module for dummy functions to test graph things with.
"""
from typing import Dict

# we import this to check we don't pull in this function when parsing this module.
from tests.resources import only_import_me


def A(b: int, c: int) -> Dict:
    """Function that outputs a typing type."""
    return {"a": b + c}


def _do_not_import_me(some_input: int, some_input2: int) -> int:
    """Function that should not become part of the graph - _do_not_import_me."""
    only_import_me.this_is_not_something_we_should_import()
    return some_input + some_input2


def B(A: dict) -> int:
    """Function that depends on A, but says it's a primitive type dict."""
    return A["a"] + 1


def A2(x: int, y: int) -> dict:
    """Graph function using primitive output type."""
    return {"a": x + y}


def B2(A2: Dict) -> int:
    """Graph function depending on A2 but saying it's a typing type."""
    return A["a"] + 1
