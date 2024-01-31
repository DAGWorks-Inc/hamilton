"""
Module for more dummy functions to test graph things with.
"""

# we import this to check we don't pull in this function when parsing this module.
from tests.resources import only_import_me  # noqa: F401


def A(b, c) -> int:
    """Should error out because we're missing an annotation"""
    return b + c


def B(b: int, c: int):
    """Should error out because we're missing an annotation"""
    return b + c


def C(b: int) -> dict:
    """Setting up type mismatch."""
    return {"hi": "world"}


def D(C: int) -> int:
    """C is the incorrect type."""
    return int(C["hi"])
