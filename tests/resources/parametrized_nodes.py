from hamilton.function_modifiers import parameterize_values


@parameterize_values(
    "param",
    {("parametrized_1", "doc"): 1, ("parametrized_2", "doc"): 2, ("parametrized_3", "doc"): 3},
)
def to_parametrize(param: int) -> int:
    """Function that should be parametrized to form multiple functions"""
    return param
