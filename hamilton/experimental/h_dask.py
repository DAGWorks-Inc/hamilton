import logging
import typing

import pandas as pd
import numpy as np

from dask import compute
from dask.delayed import Delayed, delayed
from dask.distributed import Client as DaskClient
import dask.dataframe
import dask.array


from hamilton import node
from hamilton import base


logger = logging.getLogger(__name__)


class DaskGraphAdapter(base.HamiltonGraphAdapter):
    """Class representing what's required to make Hamilton run on Dask

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, dask_client: DaskClient, result_builder: base.ResultMixin = None, visualize: bool = True):
        """Constructor

        :param dask_client: the dask client -- we don't do anything with it, but thought that it would be useful
            to wire through here.
        :param result_builder: The function that will build the result. Optional, defaults to pandas dataframe.
        :param visualize: whether we want to visualize what Dask wants to execute.
        """
        self.client = dask_client
        self.result_builder = result_builder if result_builder else base.PandasDataFrameResult()
        self.visualize = visualize

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # NOTE: the type of dask Delayed is unknown until they are computed
        if isinstance(input_value, Delayed):
            return True
        elif node_type == pd.Series and isinstance(input_value, dask.dataframe.Series):
            return True
        elif node_type == np.array and isinstance(input_value, dask.array.Array):
            return True
        return node_type == typing.Any or isinstance(input_value, node_type)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        if node_type == dask.array.Array and input_type == pd.Series:
            return True
        elif node_type == dask.dataframe.Series and input_type == pd.Series:
            return True
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return delayed(node.callable)(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Any:
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
        delayed_combine = delayed(self.result_builder.build_result)(**columns)
        if self.visualize:
            delayed_combine.visualize()
        df, = compute(delayed_combine)
        return df


# TODO: add ResultMixins for dask types
