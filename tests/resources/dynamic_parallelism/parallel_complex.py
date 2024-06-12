from hamilton.htypes import Collect, Parallelizable


def number_of_steps() -> int:
    return 5


def param_external_to_block() -> int:
    return 3


def second_param_external_to_block() -> int:
    return 4


def steps(number_of_steps: int) -> Parallelizable[int]:
    yield from range(number_of_steps)


# Parallelizable block Start


def step_modified(steps: int, second_param_external_to_block: int) -> int:
    return steps + second_param_external_to_block


def double_step(step_modified: int) -> int:
    return step_modified * 2


def triple_step(step_modified: int) -> int:
    return step_modified * 3


def double_plus_triple_step(double_step: int, triple_step: int) -> int:
    return double_step + triple_step


def double_plus_triple_plus_param_external_to_block(
    double_plus_triple_step: int, param_external_to_block: int
) -> int:
    return double_plus_triple_step + param_external_to_block


# Parallelizable block ends here


def sum_of_some_things(double_plus_triple_plus_param_external_to_block: Collect[int]) -> int:
    return sum(double_plus_triple_plus_param_external_to_block)


def final(sum_of_some_things: int) -> int:
    return sum_of_some_things
