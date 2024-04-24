from hamilton.function_modifiers import config


@config.when(foo="bar")
def b__1(a: int) -> int:
    return a + 1


@config.when(foo="baz")
def b__2(a: int) -> int:
    return a + 2


def c(b: int, should_fail: bool = False) -> int:
    if should_fail:
        raise ValueError("This is a test")
    return b + 3
