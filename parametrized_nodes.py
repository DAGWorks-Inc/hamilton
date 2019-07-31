from hamilton.function_modifiers import parametrized


@parametrized('param', {1: 'parametrized_1', 2: 'parametrized_2', 3: 'parametrized_3'})
def to_parametrize(param: int) -> int:
    """Function that should be parametrized to form multiple functions"""
    return param
