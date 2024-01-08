from hamilton.function_modifiers import extract_fields, tag


@tag(test="a")
def a() -> int:
    return 0


@tag(test="b_c")
@extract_fields({"b": int, "c": str})
def b_c(a: int) -> dict:
    return {"b": a, "c": str(a)}


@tag(test_list=["us", "uk"])
def d(a: int) -> int:
    return a
