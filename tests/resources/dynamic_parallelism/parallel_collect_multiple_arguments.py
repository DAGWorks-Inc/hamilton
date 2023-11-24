import collections
import functools

from hamilton.htypes import Collect, Parallelizable

_fn_call_counter = collections.Counter()


def _track_fn_call(fn) -> callable:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        _fn_call_counter[fn.__name__] += 1
        return fn(*args, **kwargs)

    return wrapped


def _reset_counter():
    _fn_call_counter.clear()


@_track_fn_call
def not_to_repeat() -> int:
    return -1


@_track_fn_call
def number_to_repeat(iterations: int) -> Parallelizable[int]:
    for i in range(iterations):
        yield i


@_track_fn_call
def something_else_not_to_repeat() -> int:
    return -2


@_track_fn_call
def double(number_to_repeat: int) -> int:
    return number_to_repeat * 2


@_track_fn_call
def summed(double: Collect[int], not_to_repeat: int, something_else_not_to_repeat: int) -> int:
    return sum(double) + not_to_repeat + something_else_not_to_repeat
