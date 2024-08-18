from typing import Any

from hamilton.htypes import Collect, Parallelizable


def mapper(
    drivers: list,
    inputs: list,
    final_vars: list = None,
) -> Parallelizable[dict]:
    if final_vars is None:
        final_vars = []
    for dr, input_ in zip(drivers, inputs):
        yield {
            "dr": dr,
            "final_vars": final_vars or dr.list_available_variables(),
            "input": input_,
        }


def inside(mapper: dict) -> dict:
    _dr = mapper["dr"]
    _inputs = mapper["input"]
    _final_var = mapper["final_vars"]
    return _dr.execute(final_vars=_final_var, inputs=_inputs)


def passthrough(inside: dict) -> dict:
    return inside


def reducer(passthrough: Collect[dict]) -> Any:
    return passthrough
