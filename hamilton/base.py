"""This module contains base constructs for executing a hamilton graph.

It should only import hamilton.node, numpy, pandas.
"""
import abc
import typing
import logging

import pandas as pd
import numpy as np

from . import node

logger = logging.getLogger(__name__)


class ResultMixin(object):
    @staticmethod
    @abc.abstractmethod
    def build_result(**columns: typing.Dict[str, typing.Any]) -> typing.Any:
        """This function builds the result given the computed values."""
        pass


class DictResult(ResultMixin):
    """Simple function that returns the dict of column -> value results."""
    @staticmethod
    def build_result(**columns: typing.Dict[str, typing.Any]) -> pd.DataFrame:
        return columns


class PandasDataFrameResult(ResultMixin):
    """Mixin for building a pandas dataframe from the result"""

    @staticmethod
    def build_result(**columns: typing.Dict[str, typing.Any]) -> pd.DataFrame:
        # TODO check inputs are pd.Series, arrays, or scalars -- else error
        # TODO do a basic index check across pd.Series?
        return pd.DataFrame(columns)


class NumpyMatrixResult(ResultMixin):
    """Mixin for building a Numpy Matrix from the result"""

    @staticmethod
    def build_result(**columns: typing.Dict[str, typing.Any]) -> np.matrix:
        # TODO check inputs are all numpy arrays -- else error

        num_rows = -1
        columns_with_lengths = {}
        for col, val in columns.items():
            if isinstance(val, (int, float)):  # TODO add more things here
                columns_with_lengths[(col, 1)] = val
            length = len(val)
            if num_rows == -1:
                num_rows = length
            elif length == num_rows:
                # we're good
                pass
            else:
                raise ValueError(f'Error, got non scalar result that mismatches length of other vector. '
                                 f'Got {length} for {col} instead of {num_rows}.')
            columns_with_lengths[(col, num_rows)] = val
        list_of_columns = []
        for (col, length), val in columns.items():
            if length != num_rows and length == 1:
                list_of_columns.append([val] * num_rows)
            elif length == num_rows:
                list_of_columns.append(list(val))
            else:
                raise ValueError(f'Do not know how to make this column {col} with length {length }have {num_rows} rows')
        # Create the matrix with columns as rows and then transpose
        return np.asmatrix(list_of_columns).T


class HamiltonExecutor(ResultMixin):
    @abc.abstractmethod
    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        """Needs to check whether the inputs match what the executor & functions can handle."""
        pass

    @abc.abstractmethod
    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Given a node that represents a hamilton function, execute it.

        Note, in some executors this might just return some type of "future".

        :param node:
        :param kwargs:
        :return:
        """
        pass


class SimplePythonExecutor(HamiltonExecutor):
    """The original executor for Hamilton. """

    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        return node.type == typing.Any or isinstance(input, node.type)

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return node.callable(**kwargs)
