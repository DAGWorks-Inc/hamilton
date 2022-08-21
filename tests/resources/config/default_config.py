_VALUE = "value"


def key() -> str:
    return _VALUE


def double_key(key: str) -> str:
    return key + key
