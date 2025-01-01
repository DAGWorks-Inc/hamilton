import time


def a() -> str:
    print("a")
    time.sleep(3)
    return "a"


def b() -> str:
    print("b")
    time.sleep(3)
    return "b"


def c(a: str, b: str) -> str:
    print("c")
    time.sleep(3)
    return a + " " + b


def d() -> str:
    print("d")
    time.sleep(3)
    return "d"


def e(c: str, d: str) -> str:
    print("e")
    time.sleep(3)
    return c + " " + d


def z() -> str:
    print("z")
    time.sleep(3)
    return "z"


def y() -> str:
    print("y")
    time.sleep(3)
    return "y"


def x(z: str, y: str) -> str:
    print("x")
    time.sleep(3)
    return z + " " + y


def s(x: str, e: str) -> str:
    print("s")
    time.sleep(3)
    return x + " " + e
