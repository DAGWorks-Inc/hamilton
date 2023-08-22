import asyncio
from typing import Dict

import pandas as pd

import hamilton.function_modifiers


async def simple_async_func(external_input: int) -> int:
    await asyncio.sleep(0.01)
    return external_input + 1


async def async_func_with_param(simple_async_func: int, external_input: int) -> int:
    await asyncio.sleep(0.01)
    return simple_async_func + external_input + 1


def simple_non_async_func(simple_async_func: int, async_func_with_param: int) -> int:
    return simple_async_func + async_func_with_param + 1


async def another_async_func(simple_non_async_func: int) -> int:
    await asyncio.sleep(0.01)
    return simple_non_async_func + 1


@hamilton.function_modifiers.extract_fields(dict(result_1=int, result_2=int))
def non_async_func_with_decorator(
    async_func_with_param: int, another_async_func: int
) -> Dict[str, int]:
    return {"result_1": another_async_func + 1, "result_2": async_func_with_param + 1}


@hamilton.function_modifiers.extract_columns(*["a", "b"])
async def return_df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@hamilton.function_modifiers.extract_fields(dict(result_3=int, result_4=int))
async def return_dict() -> dict:
    return {"result_3": 1, "result_4": 2}
