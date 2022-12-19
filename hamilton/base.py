"""This module contains base constructs for executing a hamilton graph.
It should only import hamilton.node, numpy, pandas.
It cannot import hamilton.graph, or hamilton.driver.
"""
import abc
import collections
import inspect
import logging
from typing import Any, Dict, List, Tuple, Type, Union

import numpy as np
import pandas as pd
import typing_inspect
from pandas.core.indexes import extension as pd_extension

from . import node

logger = logging.getLogger(__name__)


class ResultMixin(object):
    """Base class housing the static function.

    Why a static function? That's because certain frameworks can only pickle a static function, not an entire
    object.
    """

    @staticmethod
    @abc.abstractmethod
    def build_result(**outputs: Dict[str, Any]) -> Any:
        """This function builds the result given the computed values."""
        pass


class DictResult(ResultMixin):
    """Simple function that returns the dict of column -> value results."""

    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> Dict:
        """This function builds a simple dict of output -> computed values."""
        return outputs


class PandasDataFrameResult(ResultMixin):
    """Mixin for building a pandas dataframe from the result"""

    @staticmethod
    def pandas_index_types(
        outputs: Dict[str, Any]
    ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]], Dict[str, List[str]]]:
        """This function creates three dictionaries according to whether there is an index type or not.

        The three dicts we create are:
        1. Dict of index type to list of outputs that match it.
        2. Dict of time series / categorical index types to list of outputs that match it.
        3. Dict of `no-index` key to list of outputs with no index type.

        :param outputs: the dict we're trying to create a result from.
        :return: dict of all index types, dict of time series/categorical index types, dict if there is no index
        """
        all_index_types = collections.defaultdict(list)
        time_indexes = collections.defaultdict(list)
        no_indexes = collections.defaultdict(list)

        def index_key_name(pd_object: Union[pd.DataFrame, pd.Series]) -> str:
            """Creates a string helping identify the index and it's type.
            Useful for disambiguating time related indexes."""
            return f"{pd_object.index.__class__.__name__}:::{pd_object.index.dtype}"

        def get_parent_time_index_type():
            """Helper to pull the right time index parent class."""
            if hasattr(
                pd_extension, "NDArrayBackedExtensionIndex"
            ):  # for python 3.7+ & pandas >= 1.2
                index_type = pd_extension.NDArrayBackedExtensionIndex
            elif hasattr(pd_extension, "ExtensionIndex"):  # for python 3.6 & pandas <= 1.2
                index_type = pd_extension.ExtensionIndex
            else:
                index_type = None  # weird case, but not worth breaking for.
            return index_type

        for output_name, output_value in outputs.items():
            if isinstance(
                output_value, (pd.DataFrame, pd.Series)
            ):  # if it has an index -- let's grab it's type
                dict_key = index_key_name(output_value)
                if isinstance(output_value.index, get_parent_time_index_type()):
                    # it's a time index -- these will produce garbage if not aligned properly.
                    time_indexes[dict_key].append(output_name)
            elif isinstance(
                output_value, pd.Index
            ):  # there is no index on this - so it's just an integer one.
                int_index = pd.Series(
                    [1, 2, 3], index=[0, 1, 2]
                )  # dummy to get right values for string.
                dict_key = index_key_name(int_index)
            else:
                dict_key = "no-index"
                no_indexes[dict_key].append(output_name)
            all_index_types[dict_key].append(output_name)
        return all_index_types, time_indexes, no_indexes

    @staticmethod
    def check_pandas_index_types_match(
        all_index_types: Dict[str, List[str]],
        time_indexes: Dict[str, List[str]],
        no_indexes: Dict[str, List[str]],
    ) -> bool:
        """Checks that pandas index types match.

        This only logs warning errors, and if debug is enabled, a debug statement to list index types.
        """
        no_index_length = len(no_indexes)
        time_indexes_length = len(time_indexes)
        all_indexes_length = len(all_index_types)
        number_with_indexes = all_indexes_length - no_index_length
        types_match = True  # default to True
        # if there is more than one time index
        if time_indexes_length > 1:
            logger.warning(
                "WARNING: Time/Categorical index type mismatches detected - check output to ensure Pandas "
                "is doing what you intend to do. Else change the index types to match. Set logger to debug "
                "to see index types."
            )
            types_match = False
        # if there is more than one index type and it's not explained by the time indexes then
        if number_with_indexes > 1 and all_indexes_length > time_indexes_length:
            logger.warning(
                "WARNING: Multiple index types detected - check output to ensure Pandas is "
                "doing what you intend to do. Else change the index types to match. Set logger to debug to "
                "see index types."
            )
            types_match = False
        elif number_with_indexes == 1 and no_index_length > 0:
            logger.warning(
                f"WARNING: a single pandas index was found, but there are also {len(no_indexes['no-index'])} "
                "outputs without an index. Please check whether the dataframe created matches what what you "
                "expect to happen."
            )
            # Strictly speaking the index types match -- there is only one -- so setting to True.
            types_match = True
        # if all indexes matches no indexes
        elif no_index_length == all_indexes_length:
            logger.warning(
                "It appears no Pandas index type was detected (ignore this warning if you're using DASK for now.) "
                "Please check whether the dataframe created matches what what you expect to happen."
            )
            types_match = False
        if logger.isEnabledFor(logging.DEBUG):
            import pprint

            pretty_string = pprint.pformat(dict(all_index_types))
            logger.debug(f"Index types encountered:\n{pretty_string}.")
        return types_match

    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> pd.DataFrame:
        # TODO check inputs are pd.Series, arrays, or scalars -- else error
        output_index_type_tuple = PandasDataFrameResult.pandas_index_types(outputs)
        # this next line just log warnings
        # we don't actually care about the result since this is the current default behavior.
        PandasDataFrameResult.check_pandas_index_types_match(*output_index_type_tuple)

        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, pd.DataFrame):
                return value

        if not any(pd.api.types.is_list_like(value) for value in outputs.values()):
            # If we're dealing with all values that don't have any "index" that could be created
            # (i.e. scalars, objects) coerce the output to a single-row, multi-column dataframe.
            return pd.DataFrame([outputs])

        return pd.DataFrame(outputs)


class StrictIndexTypePandasDataFrameResult(PandasDataFrameResult):
    """A ResultBuilder that produces a dataframe only if the index types match exactly.

    Note: If there is no index type on some outputs, e.g. the value is a scalar, as long as there exists a single pandas
     index type, no error will be thrown, because a dataframe can be easily created.

    To use:
    from hamilton import base, driver
    strict_builder = base.StrictIndexTypePandasDataFrameResult()
    adapter = base.SimplePythonGraphAdapter(strict_builder)
    ...
    dr =  driver.Driver(config, *modules, adapter=adapter)
    df = dr.execute(...)  # this will now error if index types mismatch.
    """

    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> pd.DataFrame:
        # TODO check inputs are pd.Series, arrays, or scalars -- else error
        output_index_type_tuple = PandasDataFrameResult.pandas_index_types(outputs)
        indexes_match = PandasDataFrameResult.check_pandas_index_types_match(
            *output_index_type_tuple
        )
        if not indexes_match:
            import pprint

            pretty_string = pprint.pformat(dict(output_index_type_tuple[0]))
            raise ValueError(
                "Error: pandas index types did not match exactly. "
                f"Found the following indexes:\n{pretty_string}"
            )

        return PandasDataFrameResult.build_result(**outputs)


class NumpyMatrixResult(ResultMixin):
    """Mixin for building a Numpy Matrix from the result of walking the graph.

    All inputs to the build_result function are expected to be numpy arrays
    """

    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> np.matrix:
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
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        """Used to check whether the user inputs match what the execution strategy & functions can handle.

        :param node_type: The type of the node.
        :param input_value: An actual value that we want to inspect matches our expectation.
        :return:
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        """Used to check whether two types are equivalent.

        This is used when the function graph is being created and we're statically type checking the annotations
        for compatibility.

        :param node_type: The type of the node.
        :param input_type: The type of the input that would flow into the node.
        :return:
        """
        pass

    @abc.abstractmethod
    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
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
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        if node_type == Any:
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
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        return node.callable(**kwargs)


class SimplePythonGraphAdapter(SimplePythonDataFrameGraphAdapter):
    """This class allows you to swap out the build_result very easily."""

    def __init__(self, result_builder: ResultMixin):
        self.result_builder = result_builder
        if self.result_builder is None:
            raise ValueError("You must provide a ResultMixin object for `result_builder`.")

    def build_result(self, **outputs: Dict[str, Any]) -> Any:
        """Delegates to the result builder function supplied."""
        return self.result_builder.build_result(**outputs)
