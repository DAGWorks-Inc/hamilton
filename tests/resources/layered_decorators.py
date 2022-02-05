from hamilton.function_modifiers import config, does, parametrized, augment


def _sum(**kwargs: int) -> int:
    return sum(kwargs.values())


@does(_sum)
@parametrized(parameter='a', assigned_output={('e', 'First value'): 10, ('f', 'First value'): 20})
@augment('c**2+d')
@config.when(foo='bar')
def c__foobar(a: int, b: int) -> int:
    """Demonstrates utilizing a bunch of decorators.
    In all, this outputs two total nodes.
    - config.when makes it only apply when foo=bar
    - @does makes it do the sum pattern
    - @parametrized curries the function then turns it into two
    - @augment changes the function to make each of those two final nodes take in vars c and d
    """
    pass


@does(_sum)
@parametrized(parameter='a', assigned_output={('e', 'First value'): 11, ('f', 'First value'): 22})
@augment('c**2+d')
@config.when(foo='baz')
def c__foobaz(a: int, b: int) -> int:
    """Demonstrates utilizing a bunch of decorators.
    In all, this outputs two total nodes.
    - config.when makes it only apply when foo=bar
    - @does makes it do the sum pattern
    - @parametrized curries the function then turns it into two
    - @augment changes the function to make each of those two final nodes take in vars c and d
    """
    pass
