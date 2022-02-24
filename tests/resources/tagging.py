from hamilton.function_modifiers import tag, extract_fields


@tag(test='a')
def a() -> int:
    return 0


@tag(test='b_c')
@extract_fields({'b': int, 'c': str})
def b_c(a: int) -> dict:
    return {'b': a, 'c': str(a)}


def d(a: int) -> int:
    return a
