def A() -> int:
    """No dependencies"""
    return 3


def B(A: int) -> float:
    """Node dependency"""
    return float(A)


def C(external: str) -> str:
    """External required dependency"""
    return external


def D(default: int = 0) -> int:
    """External optional dependency"""
    return default
