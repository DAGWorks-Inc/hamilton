from hamilton.htypes import Collect, Sequential


# input
def input_number_of_steps() -> int:
    return 5


# expand
def expand_steps(number_of_steps: int) -> Sequential[int]:
    for i in range(number_of_steps):
        yield 1


# process
def squared(steps: int) -> int:
    return steps**2


# process
def cubed(steps: int) -> int:
    return steps**3


# join
def collect_sum_step_squared(squared: Collect[int], cubed: Collect[int]) -> int:
    out = 0
    for square, cube in zip(squared, cubed):
        out += square + cube
    return out


# final
def final(collect_sum_steps_squared: int) -> int:
    return collect_sum_steps_squared


def _calc():
    number_of_steps_ = input_number_of_steps()
    # Can't just instantiate it once cause its a generator
    squared_ = [squared(step) for step in expand_steps(number_of_steps_)]
    cubed_ = [cubed(step) for step in expand_steps(number_of_steps_)]
    return collect_sum_step_squared(squared_, cubed_)
