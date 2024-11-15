import asyncio

import pandas as pd

from hamilton.function_modifiers import pipe_input, pipe_output, step

# async def data_input() -> pd.DataFrame:
#     await asyncio.sleep(0.0001)
#     return


async def _groupby_a(d: pd.DataFrame) -> pd.DataFrame:
    await asyncio.sleep(0.0001)
    return d.groupby("a").sum().reset_index()


async def _groupby_b(d: pd.DataFrame) -> pd.DataFrame:
    await asyncio.sleep(0.0001)
    return d.groupby("b").sum().reset_index()


@pipe_input(
    step(_groupby_a).when(groupby="a"),
    step(_groupby_b).when_not(groupby="a"),
)
def data_pipe_input(data_input: pd.DataFrame) -> pd.DataFrame:
    return data_input


@pipe_output(
    step(_groupby_a).when(groupby="a"),
    step(_groupby_b).when_not(groupby="a"),
)
def data_pipe_output(data_input: pd.DataFrame) -> pd.DataFrame:
    return data_input
