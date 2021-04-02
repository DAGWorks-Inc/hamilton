from hamilton.function_modifiers import parametrized_input


def input_1() -> int:
    return 1


def input_2() -> int:
    return 2


@parametrized_input(
    parameter='input_value_tbd',
    assigned_inputs={
        'input_1': ('output_1', 'function_with_multiple_inputs called using input_1'),
        'input_2': ('output_2', 'function_with_multiple_inputs called using input_2')
    }
)
def function_with_multiple_inputs(input_value_tbd: int, static_value: int) -> int:
    return input_value_tbd + static_value
