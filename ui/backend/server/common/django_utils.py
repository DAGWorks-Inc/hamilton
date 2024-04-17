from typing import Callable

from django.db.models import QuerySet


async def amap(map_fn: Callable, query_set: QuerySet) -> list:
    return [map_fn(item) async for item in query_set]


async def alist(query_set: QuerySet) -> list:
    return [item async for item in query_set]
