def A(external: int) -> int:
    return external % 7 + 1


def B(A: int) -> float:
    return A / 4


def C(A: int, B: float) -> float:
    return A**2 + B
