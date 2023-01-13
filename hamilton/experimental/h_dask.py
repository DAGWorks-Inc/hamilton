import logging
import typing

import dask.array
import dask.dataframe
import numpy as np
import pandas as pd
from dask import compute
from dask.delayed import Delayed, delayed
from dask.distributed import Client as DaskClient

from hamilton import base, node
from hamilton.base import SimplePythonGraphAdapter

logger = logging.getLogger(__name__)


class DaskGraphAdapter(base.HamiltonGraphAdapter):
    """Class representing what's required to make Hamilton run on Dask

    Use `pip install sf-hamilton[dask]` to get the dependencies required to run this.

    Dask is a good choice to scale computation when you really can't do things in memory anymore with pandas. For
    most simple pandas operations, you should not have to do anything to scale!

    Dask is also a good choice if you want to scale computation generally -- you'll just have to switch to natively
    using their object types if that's the case.

    Please read the following notes about it:

    # Notes on scaling:
      - Multi-core on single machine ✅
      - Distributed computation on a Dask cluster ✅
      - Scales to any size of data supported by Dask ✅; assuming you load it appropriately via Dask loaders.

    # Function return object types supported:
     - Works for any python object that can be serialized by the Dask framework. ✅

    # Pandas?
     - Dask implements a good subset of the Pandas API:
       > You might be able to get away with scaling without having to change your code at all!
       > See https://docs.dask.org/en/latest/dataframe-api.html for Pandas supported APIs.
       > If it not supported by their API, you have to then read up and think about how to structure you hamilton
       > function computation -- https://docs.dask.org/en/latest/dataframe.html

    # Loading Data:
     - see https://docs.dask.org/en/latest/best-practices.html#load-data-with-dask
     - we recommend creating a python module specifically encapsulating functions that help you load data.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(
        self,
        dask_client: DaskClient,
        result_builder: base.ResultMixin = None,
        visualize_kwargs: dict = None,
    ):
        """Constructor

        :param dask_client: the dask client -- we don't do anything with it, but thought that it would be useful
            to wire through here.
        :param result_builder: The function that will build the result. Optional, defaults to pandas dataframe.
        :param visualize_kwargs: Arguments to visualize the graph using dask's internals.
            None, means no visualization.
            Dict, means visualize -- see https://docs.dask.org/en/latest/api.html?highlight=visualize#dask.visualize
            for what to pass in.
        """
        self.client = dask_client
        self.result_builder = result_builder if result_builder else base.PandasDataFrameResult()
        self.visualize_kwargs = visualize_kwargs

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # NOTE: the type of dask Delayed is unknown until they are computed
        if isinstance(input_value, Delayed):
            return True
        elif node_type == pd.Series and isinstance(input_value, dask.dataframe.Series):
            return True
        elif node_type == np.array and isinstance(input_value, dask.array.Array):
            return True
        return SimplePythonGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        if node_type == dask.array.Array and input_type == pd.Series:
            return True
        elif node_type == dask.dataframe.Series and input_type == pd.Series:
            return True
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph.
        :param kwargs: the arguments that should be passed to it.
        :return: returns a dask delayed object.
        """
        return delayed(node.callable)(**kwargs)

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[delayed object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        if logger.isEnabledFor(logging.DEBUG):
            for k, v in outputs.items():
                logger.info(f"Got column {k}, with type [{type(v)}].")
        delayed_combine = delayed(self.result_builder.build_result)(**outputs)
        if self.visualize_kwargs is not None:
            delayed_combine.visualize(**self.visualize_kwargs)
        (df,) = compute(delayed_combine)
        return df


# TODO: add ResultMixins for dask types
