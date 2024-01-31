"""
Module for functions with genercis to test graph things with.
"""

from typing import Dict, List, Mapping, Tuple


def A(b: Dict, c: int) -> Tuple[Dict, int]:
    """Function that should become part of the graph - A"""
    return b, c


def B(A: Tuple[Dict, int]) -> List:
    """Function that should become part of the graph - B"""
    return [A, A]


def C(B: list) -> Dict:
    return {"foo": B}


def D(C: Mapping) -> float:
    return 1.0
