import typing

import pandas as pd

from dask import compute
from dask.delayed import Delayed, delayed

from . import node


class DaskExecutor:
    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        # NOTE: the type of dask Delayed is unknown until they are computed
        if isinstance(input, Delayed):
            return True

        return node.type == typing.Any or isinstance(input, node.type)

    def execute(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return delayed(node.callable)(**kwargs)

    def build_data_frame(self, columns: typing.Dict[str, typing.Any]) -> pd.DataFrame:
        columns, = compute(columns)
        return pd.DataFrame(columns)
