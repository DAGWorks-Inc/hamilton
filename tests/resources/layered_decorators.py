from hamilton.function_modifiers import config, does, parametrized, augment


def _sum(**kwargs: int) -> int:
    return sum(kwargs.values())


@config.when(foo='bar')
@does(_sum)
@parametrized(parameter='a', assigned_output={('e', 'First value'): 10, ('f', 'First value'): 20})
@augment("c**2+d")
def c(a: int, b: int) -> int:
    """Demonstrates utilizing a bunch of decorators

    :param a: Some value, we'll parametrize
    :param b: Some other value, we'll parametrize
    :return: THe sum of a and b
    """
    pass
