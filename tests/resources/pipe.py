from hamilton.function_modifiers import pipe, source, step, value


def _add_one(v: int) -> int:
    return v + 1


def _add_n(v: int, n: int) -> int:
    return v + n


def _multiply_n(v: int, *, n: int) -> int:
    return v * n


def _add_two(v: int) -> int:
    return v + 2


def _square(v: int) -> int:
    return v * v


def v() -> int:
    return 10


@pipe(
    step(_add_one).named("a"),
    step(_add_two).named("b"),
    step(_add_n, n=3).named("c").when(calc_c=True),
    step(_add_n, n=source("input_1")).named("d"),
    step(_multiply_n, n=source("input_2")).named("e"),
)
def chain_1_using_pipe(v: int) -> int:
    return v * 10


def chain_1_not_using_pipe(v: int, input_1: int, input_2: int, calc_c: bool = False) -> int:
    a = _add_one(v)
    b = _add_two(a)
    c = _add_n(b, n=3) if calc_c else b
    d = _add_n(c, n=input_1)
    e = _multiply_n(d, n=input_2)
    return e * 10


@pipe(
    step(_square).named("a"),
    step(_multiply_n, n=value(2)).named("b"),
    step(_add_n, n=10).named("c").when(calc_c=True),
    step(_add_n, n=source("input_3")).named("d"),
    step(_add_two).named("e"),
)
def chain_2_using_pipe(v: int) -> int:
    return v + 10


def chain_2_not_using_pipe(v: int, input_3: int, calc_c: bool = False) -> int:
    a = _square(v)
    b = _multiply_n(a, n=2)
    c = _add_n(b, n=10) if calc_c else b
    d = _add_n(c, n=input_3)  # Assuming "upstream" refers to the same value as "v" here
    e = _add_two(d)
    return e + 10
