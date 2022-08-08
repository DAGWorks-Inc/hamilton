import asyncio
import aiohttp
import fastapi


async def request_raw(request: fastapi.Request) -> dict:
    return await request.json()


def foo(request_raw: dict) -> str:
    return request_raw.get('foo', 'far')


def bar(request_raw: dict) -> str:
    return request_raw.get('bar', 'baz')


async def computation1(foo: str, some_data: dict) -> bool:
    await asyncio.sleep(1)
    return False


async def some_data() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get('http://httpbin.org/get') as resp:
            return await resp.json()


async def computation2(bar: str) -> bool:
    await asyncio.sleep(1)
    return True


async def pipeline(computation1: bool, computation2: bool) -> dict:
    await asyncio.sleep(1)
    return {'computation1': computation1, 'computation2': computation2}
