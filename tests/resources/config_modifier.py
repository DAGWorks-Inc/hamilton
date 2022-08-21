from hamilton.function_modifiers import config


def new_param() -> str:
    return "dummy"


@config.when(fn_1_version=0)
def fn() -> str:
    pass


@config.when(fn_1_version=1)
def fn__v1() -> str:
    return "version_1"


@config.when(fn_1_version=2)
def fn__v2(new_param: str) -> str:
    return "version_2"


@config.when(fn_1_version=3, name="fn")
def fn_to_rename() -> str:
    return "version_3"
