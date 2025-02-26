from hamilton.function_modifiers import check_output, parameterize, value

from tests.resources.decorator_related import base

b_p = parameterize(b={"input": value(1)}, c={"input": value(2)})(base.a)

b_p2 = parameterize(q={"input": value(4)}, r={"input": value(5)})(base.a)

b_p3 = check_output(
    range=(0, 10),
)(base.aa)
b_p3.__name__ = "b_p3"  # required to register this as `b_p3` in the graph

b_p4 = parameterize(aaa={"input": value(4)}, aab={"input": value(5)})(base.aa)


def d(b: int, c: int) -> int:
    return b + c


def e(input: int, a: int) -> int:
    return input * 4
