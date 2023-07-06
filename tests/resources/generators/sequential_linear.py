from hamilton.htypes import Collect, Sequential


# input
def number_of_steps() -> int:
    return 5


# expand
def steps(number_of_steps: int) -> Sequential[int]:
    for i in range(number_of_steps):
        print("yielding step {}".format(i))
        yield i


# process
def step_squared(steps: int) -> int:
    print("squaring step {}".format(steps))
    return steps**2


# join
def sum_step_squared(step_squared: Collect[int]) -> int:
    print("summing step squared")
    out = 0
    for step in step_squared:
        out += step
    return out


# final
def final(sum_step_squared: int) -> int:
    print("finalizing")
    return sum_step_squared


def _calc():
    number_of_steps_ = number_of_steps()
    steps_ = steps(number_of_steps_)
    step_squared_ = [step_squared(step) for step in steps_]
    return sum_step_squared(step_squared_)
