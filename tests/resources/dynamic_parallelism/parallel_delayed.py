from hamilton.htypes import Collect, Parallelizable


# expand
def steps(number_of_steps: int) -> Parallelizable[int]:
    return list(range(number_of_steps))


# process
def step_squared(steps: int, delay_seconds: float) -> int:
    import time

    print("start compute step {}".format(steps))
    time.sleep(delay_seconds)
    print("end compute step {}".format(steps))
    # print("squaring step {}".format(steps))
    return steps**2


# process
def step_cubed(steps: int) -> int:
    print("cubing step {}".format(steps))
    return steps**3


def step_squared_plus_step_cubed(step_squared: int, step_cubed: int) -> int:
    print("adding step squared and step cubed")
    return step_squared + step_cubed


# join
def sum_step_squared_plus_step_cubed(step_squared_plus_step_cubed: Collect[int]) -> int:
    print("summing step squared")
    out = 0
    for step in step_squared_plus_step_cubed:
        out += step
    return out


# final
def final(sum_step_squared_plus_step_cubed: int) -> int:
    print("finalizing")
    return sum_step_squared_plus_step_cubed
