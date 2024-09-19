from hamilton.function_modifiers import post_pipe, source, step, value


def _post_pipe0(x: str) -> str:
    return x + "-post pipe 0-"


def _post_pipe1(x: str, upstream: str) -> str:
    return x + f"-post pipe 1 with {upstream}-"


def _post_pipe2(x: str, some_val: int) -> str:
    return x + f"-post pipe 2 with value {some_val}-"


def upstream() -> str:
    return "-upstream-"


def user_input() -> str:
    return "-user input-"


@post_pipe(
    step(_post_pipe0),
    step(_post_pipe1, upstream=source("upstream")).named("random"),
    step(_post_pipe2, some_val=value(1000)),
)
def f_of_interest(user_input: str) -> str:
    return user_input + "-raw function-"


def downstream_f(f_of_interest: str) -> str:
    return f_of_interest + "-downstream."


def chain_not_using_post_pipe() -> str:
    t = downstream_f(
        _post_pipe2(_post_pipe1(_post_pipe0(f_of_interest(user_input())), upstream()), 1000)
    )
    return t


def _add_one(v: int) -> int:
    return v + 1


def _add_two(v: int) -> int:
    return v + 2


def _add_n(v: int, n: int) -> int:
    return v + n


def _multiply_n(v: int, *, n: int) -> int:
    return v * n


def _square(v: int) -> int:
    return v * v


def v() -> int:
    return 10


@post_pipe(
    step(_add_one).named("a"),
    step(_add_two).named("b"),
    step(_add_n, n=3).named("c").when(calc_c=True),
    step(_add_n, n=source("input_1")).named("d"),
    step(_multiply_n, n=source("input_2")).named("e"),
)
def chain_1_using_post_pipe(v: int) -> int:
    return v * 10


def chain_1_not_using_post_pipe(v: int, input_1: int, input_2: int, calc_c: bool = False) -> int:
    start = v * 10
    a = _add_one(start)
    b = _add_two(a)
    c = _add_n(b, n=3) if calc_c else b
    d = _add_n(c, n=input_1)
    e = _multiply_n(d, n=input_2)
    return e


@post_pipe(
    step(_square).named("a"),
    step(_multiply_n, n=value(2)).named("b"),
    step(_add_n, n=10).named("c").when(calc_c=True),
    step(_add_n, n=source("input_3")).named("d"),
    step(_add_two).named("e"),
)
def chain_2_using_post_pipe(v: int) -> int:
    return v + 10


def chain_2_not_using_post_pipe(v: int, input_3: int, calc_c: bool = False) -> int:
    start = v + 10
    a = _square(start)
    b = _multiply_n(a, n=2)
    c = _add_n(b, n=10) if calc_c else b
    d = _add_n(c, n=input_3)  # Assuming "upstream" refers to the same value as "v" here
    e = _add_two(d)
    return e
