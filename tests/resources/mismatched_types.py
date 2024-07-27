def foo(a: int) -> float:
    return a * 0.5


def bar(a: int) -> int:
    return int(a * 2)


def baz(a: int, bar: float, number: str) -> str:
    return f"{a} {bar} {number}"
