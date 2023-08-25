from hamilton.htypes import Collect, Parallelizable


def num() -> int:
    return 10


def par(num: int) -> Parallelizable[int]:
    for i in range(num):
        yield i


def identity(par: int) -> int:
    return par


def foo() -> int:
    return 1


def collect(identity: Collect[int], foo: int) -> int:
    return sum(identity) + foo


def collect_plus_one(collect: int) -> int:
    return collect + 1
