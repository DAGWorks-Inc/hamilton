from hamilton import function_modifiers
from hamilton.experimental.decorators.reuse import MultiOutput, reuse_functions
from hamilton.function_modifiers.configuration import config


def a() -> int:
    return 1


def b() -> int:
    return 2


def _get_submodules():
    """Utility function to gather fns for reuse so they don't get included in the general function graph"""

    @config.when(op="add")
    def d__add(a: int, c: int) -> int:
        return a + c

    @config.when(op="subtract")
    def d__subtract(a: int, c: int) -> int:
        return a - c

    def e(c: int) -> int:
        return c * 2

    def f(c: int, b: int, e: int) -> int:
        return c + b + e

    submodules = [d__add, d__subtract, e, f]
    return submodules


submodules = _get_submodules()


@reuse_functions(
    with_inputs={"c": function_modifiers.value(10)},
    namespace="v1",
    outputs={"e": "e_1", "f": "f_1"},
    with_config={"op": "subtract"},
    load_from=_get_submodules(),
)
def subdag_1() -> MultiOutput(e_1=int, f_1=int):
    pass


@reuse_functions(
    with_inputs={"c": function_modifiers.value(20)},
    namespace="v2",
    outputs={"e": "e_2", "f": "f_2"},
    with_config={"op": "subtract"},
    load_from=_get_submodules(),
)
def subdag_2() -> MultiOutput(e_2=int, f_2=int):
    pass


@reuse_functions(
    with_inputs={"c": function_modifiers.value(30)},
    namespace="v3",
    outputs={"e": "e_3", "f": "f_3"},
    with_config={"op": "add"},
    load_from=_get_submodules(),
)
def subdag_3() -> MultiOutput(e_3=int, f_3=int):
    pass


@reuse_functions(
    with_inputs={"c": function_modifiers.source("b")},
    namespace="v4",
    outputs={"e": "e_4", "f": "f_4"},
    with_config={"op": "add"},
    load_from=_get_submodules(),
)
def subdag_4() -> MultiOutput(e_4=int, f_4=int):
    pass


def _sum(**kwargs: int) -> int:
    return sum(kwargs.values())


@function_modifiers.does(_sum)
def sum_everything(
    e_1: int, e_2: int, e_3: int, e_4: int, f_1: int, f_2: int, f_3: int, f_4: int
) -> int:
    pass
