import asyncio

import pandas as pd

from hamilton.function_modifiers import apply_to, mutate


def data_mutate(data_input: pd.DataFrame) -> pd.DataFrame:
    return data_input


@mutate(apply_to(data_mutate).when(groupby="a"))
async def _groupby_a_mutate(d: pd.DataFrame) -> pd.DataFrame:
    await asyncio.sleep(0.0001)
    return d.groupby("a").sum().reset_index()


@mutate(apply_to(data_mutate).when_not(groupby="a"))
async def _groupby_b_mutate(d: pd.DataFrame) -> pd.DataFrame:
    await asyncio.sleep(0.0001)
    return d.groupby("b").sum().reset_index()
