"""
Module for cyclic functions to test graph things with.
"""

# we import this to check we don't pull in this function when parsing this module.
from tests.resources import only_import_me  # noqa: F401


def A(b: int, c: int) -> int:
    """Function that should become part of the graph - A"""
    return b + c


def B(A: int, D: int) -> int:
    """Function that should become part of the graph - B"""
    return A * A + D


def C(B: int) -> int:  # empty string doc on purpose.
    return B * 2


def D(C: int) -> int:
    return C + 1
