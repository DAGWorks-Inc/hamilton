from typing import Dict

from hamilton.function_modifiers import extract_fields, pipe_output, step


def _pre_step(something: int) -> int:
    return something + 10


def _post_step(something: int) -> int:
    return something + 100


def _something_else(something: int) -> int:
    return something + 1000


def a() -> int:
    return 10


@pipe_output(
    step(_something_else),  # gets applied to all sink nodes
    step(_pre_step).named(name="transform_1").on_output("field_1"),  # only applied to field_1
    step(_post_step)
    .named(name="transform_2")
    .on_output(["field_1", "field_3"]),  # applied to field_1 and field_3
)
@extract_fields({"field_1": int, "field_2": int, "field_3": int})
def foo(a: int) -> Dict[str, int]:
    return {"field_1": 1, "field_2": 2, "field_3": 3}
