import asyncio


async def a() -> str:
    print("a")
    await asyncio.sleep(3)
    return "a"


async def b() -> str:
    print("b")
    await asyncio.sleep(3)
    return "b"


async def c(a: str, b: str) -> str:
    print("c")
    await asyncio.sleep(3)
    return a + " " + b


async def d() -> str:
    print("d")
    await asyncio.sleep(3)
    return "d"


async def e(c: str, d: str) -> str:
    print("e")
    await asyncio.sleep(3)
    return c + " " + d


async def z() -> str:
    print("z")
    await asyncio.sleep(3)
    return "z"


async def y() -> str:
    print("y")
    await asyncio.sleep(3)
    return "y"


async def x(z: str, y: str) -> str:
    print("x")
    await asyncio.sleep(3)
    return z + " " + y


async def s(x: str, e: str) -> str:
    print("s")
    await asyncio.sleep(3)
    return x + " " + e
