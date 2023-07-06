from hamilton.htypes import Collect, Sequential


# input
def number_of_steps() -> int:
    return 5


# expand
def steps(number_of_steps: int) -> Sequential[int]:
    for i in range(number_of_steps):
        yield i


# join
def count_steps(steps: Collect[int]) -> int:
    out = 0
    for i in steps:
        out += 1
    return out


# final
def final(count_steps: int) -> int:
    return count_steps


def _calc() -> int:
    return count_steps(steps(number_of_steps()))
