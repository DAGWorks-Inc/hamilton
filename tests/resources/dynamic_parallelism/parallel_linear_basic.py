from hamilton.htypes import Collect, Parallelizable


# input
def number_of_steps() -> int:
    return 6


# expand
def steps(number_of_steps: int) -> Parallelizable[int]:
    yield from range(number_of_steps)


# process
def step_squared(steps: int) -> int:
    return steps**2


# process
def step_cubed(steps: int) -> int:
    return steps**3


def step_squared_plus_step_cubed(step_squared: int, step_cubed: int) -> int:
    return step_squared + step_cubed


# join
def sum_step_squared_plus_step_cubed(step_squared_plus_step_cubed: Collect[int]) -> int:
    out = 0
    for step in step_squared_plus_step_cubed:
        out += step
    return out


# final
def final(sum_step_squared_plus_step_cubed: int) -> int:
    return sum_step_squared_plus_step_cubed


def _calc(number_of_steps: int = number_of_steps()) -> int:
    steps_ = steps(number_of_steps)
    to_sum = []
    for step_ in steps_:
        step_squared_ = step_squared(step_)
        step_cubed_ = step_cubed(step_)
        step_squared_plus_step_cubed_ = step_squared_plus_step_cubed(step_squared_, step_cubed_)
        to_sum.append(step_squared_plus_step_cubed_)
    sum_step_squared_plus_step_cubed_ = sum_step_squared_plus_step_cubed(to_sum)
    final_ = final(sum_step_squared_plus_step_cubed_)
    return final_
