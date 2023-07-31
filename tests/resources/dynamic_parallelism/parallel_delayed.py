from hamilton.htypes import Collect, Parallelizable


def steps(number_of_steps: int) -> Parallelizable[int]:
    yield from range(number_of_steps)


def step_squared(steps: int, delay_seconds: float) -> int:
    import time

    time.sleep(delay_seconds)
    return steps**2


def step_cubed(steps: int) -> int:
    return steps**3


def step_squared_plus_step_cubed(step_squared: int, step_cubed: int) -> int:
    return step_squared + step_cubed


def sum_step_squared_plus_step_cubed(step_squared_plus_step_cubed: Collect[int]) -> int:
    out = 0
    for step in step_squared_plus_step_cubed:
        out += step
    return out


def final(sum_step_squared_plus_step_cubed: int) -> int:
    return sum_step_squared_plus_step_cubed
