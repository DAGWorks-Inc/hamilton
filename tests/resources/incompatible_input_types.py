from typing import Union


def b(a: int) -> int:
    return a


def c(a: str) -> str:
    return a


def e(d: Union[int, str]) -> int:
    return d


def f(d: Union[float, int]) -> float:
    return d
