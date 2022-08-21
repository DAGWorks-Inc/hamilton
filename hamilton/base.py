"""This module contains base constructs for executing a hamilton graph.
It should only import hamilton.node, numpy, pandas.
It cannot import hamilton.graph, or hamilton.driver.
"""
import abc
import collections
import inspect
import typing

import numpy as np
import pandas as pd
import typing_inspect

from . import node


class ResultMixin(object):
    """Base class housing the static function.

    Why a static function? That's because certain frameworks can only pickle a static function, not an entire
    object.
    """

    @staticmethod
    @abc.abstractmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """This function builds the result given the computed values."""
        pass


class DictResult(ResultMixin):
    """Simple function that returns the dict of column -> value results."""

    @staticmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> typing.Dict:
        """This function builds a simple dict of output -> computed values."""
        return outputs


class PandasDataFrameResult(ResultMixin):
    """Mixin for building a pandas dataframe from the result"""

    @staticmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> pd.DataFrame:
        # TODO check inputs are pd.Series, arrays, or scalars -- else error
        # TODO do a basic index check across pd.Series and flag where mismatches occur?
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, pd.DataFrame):
                return value
            elif isinstance(value, pd.Series):
                return pd.DataFrame(outputs)
            raise ValueError(f"Cannot build result. Cannot handle type {value}.")
        return pd.DataFrame(outputs)


class NumpyMatrixResult(ResultMixin):
    """Mixin for building a Numpy Matrix from the result of walking the graph.

    All inputs to the build_result function are expected to be numpy arrays
    """

    @staticmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> np.matrix:
        """Builds a numpy matrix from the passed in, inputs.

        :param outputs: function_name -> np.array.
        :return: numpy matrix
        """
        # TODO check inputs are all numpy arrays/array like things -- else error
        num_rows = -1
        columns_with_lengths = collections.OrderedDict()
        for col, val in outputs.items():  # assumption is fixed order
            if isinstance(val, (int, float)):  # TODO add more things here
                columns_with_lengths[(col, 1)] = val
            else:
                length = len(val)
                if num_rows == -1:
                    num_rows = length
                elif length == num_rows:
                    # we're good
                    pass
                else:
                    raise ValueError(
                        f"Error, got non scalar result that mismatches length of other vector. "
                        f"Got {length} for {col} instead of {num_rows}."
                    )
                columns_with_lengths[(col, num_rows)] = val
        list_of_columns = []
        for (col, length), val in columns_with_lengths.items():
            if length != num_rows and length == 1:
                list_of_columns.append([val] * num_rows)  # expand single values into a full row
            elif length == num_rows:
                list_of_columns.append(list(val))
            else:
                raise ValueError(
                    f"Do not know how to make this column {col} with length {length }have {num_rows} rows"
                )
        # Create the matrix with columns as rows and then transpose
        return np.asmatrix(list_of_columns).T


class HamiltonGraphAdapter(ResultMixin):
    """Any GraphAdapters should implement this interface to adapt the HamiltonGraph for that particular context.

    Note since it inherits ResultMixin -- HamiltonGraphAdapters need a `build_result` function too.
    """

    @staticmethod
    @abc.abstractmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        """Used to check whether the user inputs match what the execution strategy & functions can handle.

        :param node_type: The type of the node.
        :param input_value: An actual value that we want to inspect matches our expectation.
        :return:
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        """Used to check whether two types are equivalent.

        This is used when the function graph is being created and we're statically type checking the annotations
        for compatibility.

        :param node_type: The type of the node.
        :param input_type: The type of the input that would flow into the node.
        :return:
        """
        pass

    @abc.abstractmethod
    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Given a node that represents a hamilton function, execute it.
        Note, in some adapters this might just return some type of "future".

        :param node: the Hamilton Node
        :param kwargs: the kwargs required to exercise the node function.
        :return: the result of exercising the node.
        """
        pass


class SimplePythonDataFrameGraphAdapter(HamiltonGraphAdapter, PandasDataFrameResult):
    """This is the default (original Hamilton) graph adapter. It uses plain python and builds a dataframe result."""

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        if node_type == typing.Any:
            return True
        elif inspect.isclass(node_type) and isinstance(input_value, node_type):
            return True
        elif typing_inspect.is_typevar(node_type):  # skip runtime comparison for now.
            return True
        elif typing_inspect.is_generic_type(node_type) and typing_inspect.get_origin(
            node_type
        ) == type(input_value):
            return True
        elif typing_inspect.is_union_type(node_type):
            union_types = typing_inspect.get_args(node_type)
            return any(
                [
                    SimplePythonDataFrameGraphAdapter.check_input_type(ut, input_value)
                    for ut in union_types
                ]
            )
        elif node_type == type(input_value):
            return True
        return False

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return node.callable(**kwargs)


class SimplePythonGraphAdapter(SimplePythonDataFrameGraphAdapter):
    """This class allows you to swap out the build_result very easily."""

    def __init__(self, result_builder: ResultMixin):
        self.result_builder = result_builder
        if self.result_builder is None:
            raise ValueError("You must provide a ResultMixin object for `result_builder`.")

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Delegates to the result builder function supplied."""
        return self.result_builder.build_result(**outputs)
