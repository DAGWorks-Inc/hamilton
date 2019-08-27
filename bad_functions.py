"""
Module for more dummy functions to test graph things with.
"""
# we import this to check we don't pull in this function when parsing this module.
from tests.resources import only_import_me


def A(b, c) -> int:
    """Should error out because we're missing an annotation"""
    return b + c


def B(b: int, c: int):
    """Should error out because we're missing an annotation"""
    return b + c
