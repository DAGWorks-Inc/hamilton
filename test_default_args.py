"""
Module for dummy functions to test graph things with.
"""


def A(required: int, defaults_to_zero: int = 0) -> int:
    """Function that should become part of the graph - A"""
    return required + defaults_to_zero


def B(A: int) -> int:
    """Function that should become part of the graph - B"""
    return A * A


def C(A: int) -> int:  # empty string doc on purpose.
    return A * 2
