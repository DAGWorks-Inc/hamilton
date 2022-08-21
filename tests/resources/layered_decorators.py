from hamilton.function_modifiers import config, does, parameterize_values

"""Demonstrates a DAG with multiple decorators for functions.
This is a good test case to ensure that all the decorators work together
This DAG outputs two nodes -- e and f. The value of these will vary
based on whether or not foo==bar or foo==baz in the config.
"""


def _sum(**kwargs: int) -> int:
    return sum(kwargs.values())


@does(_sum)
@parameterize_values(
    parameter="a", assigned_output={("e", "First value"): 10, ("f", "Second value"): 20}
)
@config.when(foo="bar")
def c__foobar(a: int, b: int) -> int:
    """Demonstrates utilizing a bunch of decorators.
    In all, this outputs two total nodes -- e and f (as its parametrized)
    - config.when makes it only apply when foo=bar
    - @does makes it do the sum pattern
    - @parametrized curries the function then turns it into two
    """
    pass


@does(_sum)
@parameterize_values(
    parameter="a", assigned_output={("e", "First value"): 11, ("f", "Second value"): 22}
)
@config.when(foo="baz")
def c__foobaz(a: int, b: int) -> int:
    """Demonstrates utilizing a bunch of decorators.
    In all, this outputs two total nodes -- e and f (as its parametrized)
    - config.when makes it only apply when foo=bar
    - @does makes it do the sum pattern
    - @parametrized curries the function then turns it into two
    """
    pass
