from hamilton.function_modifiers import mutate


def data_1() -> int:
    return 10


@mutate(data_1)
def add_something(user_input: int) -> int:
    return user_input + 100


@mutate(data_1)
def add_something_more(user_input: int) -> int:
    return user_input + 1000


@mutate(data_1)
def add_something(user_input: int) -> int:  # noqa
    return user_input + 100
