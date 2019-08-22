def key_overwrite_not_ignoring_input(key_to_overwrite: str) -> str:
    return key_to_overwrite


def key_overwrite_ignoring_input(key_to_overwrite_2: str) -> str:
    return key_to_overwrite_2


def key_fn_not_to_overwrite() -> str:
    return 'default_value'


def key_not_to_overwrite_with_params(key_not_to_overwrite: str) -> str:
    return 'default_value'
