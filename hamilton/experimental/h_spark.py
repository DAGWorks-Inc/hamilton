import functools
import inspect
import logging
from typing import Any, Callable, Dict, Set, Tuple, Type, Union

import numpy as np
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import DataFrame, dataframe, types
from pyspark.sql.functions import column, lit, pandas_udf, udf

from hamilton import base, htypes, node

logger = logging.getLogger(__name__)


class KoalasDataFrameResult(base.ResultMixin):
    """Mixin for building a koalas dataframe from the result"""

    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> ps.DataFrame:
        """Right now this class is just used for signaling the return type."""
        pass


class SparkKoalasGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Spark with Koalas, i.e. Pandas on Spark.

    This walks the graph and translates it to run onto `Apache Spark <https://spark.apache.org/">`__ \
    using the \
    `Pandas API on Spark <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html>`__

    Use `pip install sf-hamilton[spark]` to get the dependencies required to run this.

    Currently, this class assumes you're running SPARK 3.2+. You'd generally use this if you have an existing spark \
    cluster running in your workplace, and you want to scale to very large data set sizes.

    Some tips on koalas (before it was merged into spark 3.2):

     - https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
     - https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html

    Spark is a more heavyweight choice to scale computation for Hamilton graphs creating a Pandas Dataframe.

    Notes on scaling:
    -----------------
      - Multi-core on single machine ✅ (if you setup Spark locally to do so)
      - Distributed computation on a Spark cluster ✅
      - Scales to any size of data as permitted by Spark ✅

    Function return object types supported:
    ---------------------------------------
      - ⛔ Not generic. This does not work for every Hamilton graph.
      - ✅ Currently we're targeting this at Pandas/Koalas types [dataframes, series].

    Pandas?
    -------
      - ✅ Koalas on Spark 3.2+ implements a good subset of the pandas API. Keep it simple and you should be good to go!

    CAVEATS
    -------
      - Serialization costs can outweigh the benefits of parallelism, so you should benchmark your code to see if it's\
      worth it.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, spark_session, result_builder: base.ResultMixin, spine_column: str):
        """Constructor

        You only have the ability to return either a Pandas on Spark Dataframe or a Pandas Dataframe. To do that you \
        either use the stock \
        `base.PandasDataFrameResult <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/base.py#L39>`__ class,\
         or you use `h_spark.KoalasDataframeResult <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/experimental/h_spark.py#L16>`__.

        :param spark_session: the spark session to use.
        :param result_builder: the function to build the result -- currently on Pandas and Koalas are "supported".
        :param spine_column: the column we should use first as the spine and then subsequently join against.
        """
        self.spark_session = spark_session
        if not (
            isinstance(result_builder, base.PandasDataFrameResult)
            or isinstance(result_builder, KoalasDataFrameResult)
            or isinstance(result_builder, base.DictResult)
        ):
            raise ValueError(
                "SparkKoalasGraphAdapter only supports returning:"
                ' a "pandas" DF at the moment, a "koalas" DF at the moment, or a "dict" of results.'
            )
        self.result_builder = result_builder
        self.spine_column = spine_column

    @staticmethod
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        """Function to equate an input value, with expected node type.

        We need this to equate pandas and koalas objects/types.

        :param node_type: the declared node type
        :param input_value: the actual input value
        :return: whether this is okay, or not.
        """
        # TODO: flesh this out more
        if (node_type == pd.Series or node_type == ps.Series) and (
            isinstance(input_value, ps.DataFrame) or isinstance(input_value, ps.Series)
        ):
            return True
        elif node_type == np.array and isinstance(input_value, dataframe.DataFrame):
            return True

        return base.SimplePythonGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        """Function to help equate pandas with koalas types.

        :param node_type: the declared node type.
        :param input_type: the type of what we want to pass into it.
        :return: whether this is okay, or not.
        """
        if node_type == ps.Series and input_type == pd.Series:
            return True
        elif node_type == pd.Series and input_type == ps.Series:
            return True
        elif node_type == ps.DataFrame and input_type == pd.DataFrame:
            return True
        elif node_type == pd.DataFrame and input_type == ps.DataFrame:
            return True
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph.
        :param kwargs: the arguments that should be passed to it.
        :return: returns a koalas column
        """
        return node.callable(**kwargs)

    def build_result(self, **outputs: Dict[str, Any]) -> Union[pd.DataFrame, ps.DataFrame, dict]:
        if isinstance(self.result_builder, base.DictResult):
            return self.result_builder.build_result(**outputs)
        # we don't use the actual function for building right now, we use this hacky equivalent
        df = ps.DataFrame(outputs[self.spine_column])
        for k, v in outputs.items():
            logger.info(f"Got column {k}, with type [{type(v)}].")
            df[k] = v
        if isinstance(self.result_builder, base.PandasDataFrameResult):
            return df.to_pandas()
        else:
            return df


def numpy_to_spark_type(numpy_type: Type) -> types.DataType:
    """Function to convert a numpy type to a Spark type.

    :param numpy_type: the numpy type to convert.
    :return: the Spark type.
    :raise: ValueError if the type is not supported.
    """
    if (
        numpy_type == np.int8
        or numpy_type == np.int16
        or numpy_type == np.int32
        or numpy_type == np.int64
    ):
        return types.IntegerType()
    elif numpy_type == np.float16 or numpy_type == np.float32 or numpy_type == np.float64:
        return types.FloatType()
    elif numpy_type == np.bool_:
        return types.BooleanType()
    elif numpy_type == np.unicode_ or numpy_type == np.string_:
        return types.StringType()
    elif numpy_type == np.bytes_:
        return types.BinaryType()
    else:
        raise ValueError("Unsupported NumPy type: " + str(numpy_type))


def python_to_spark_type(python_type: Union[int, float, bool, str, bytes]) -> types.DataType:
    """Function to convert a Python type to a Spark type.

    :param python_type: the Python type to convert.
    :return: the Spark type.
    :raise: ValueError if the type is not supported.
    """
    if python_type == int:
        return types.IntegerType()
    elif python_type == float:
        return types.FloatType()
    elif python_type == bool:
        return types.BooleanType()
    elif python_type == str:
        return types.StringType()
    elif python_type == bytes:
        return types.BinaryType()
    else:
        raise ValueError("Unsupported Python type: " + str(python_type))


def get_spark_type(
    actual_kwargs: dict, df: DataFrame, hamilton_udf: Callable, return_type: Any
) -> types.DataType:
    if return_type in (int, float, bool, str, bytes):
        return python_to_spark_type(return_type)
    elif hasattr(return_type, "__module__") and getattr(return_type, "__module__") == "numpy":
        return numpy_to_spark_type(return_type)
    else:
        logger.debug(f"{inspect.signature(hamilton_udf)}, {actual_kwargs}, {df.columns}")
        raise ValueError(
            f"Currently unsupported return type {return_type}. "
            f"Please create an issue or PR to add support for this type."
        )


def _get_pandas_annotations(hamilton_udf: Callable) -> Dict[str, bool]:
    """Given a function, return a dictionary of the parameters that are annotated as pandas series.

    :param hamilton_udf: the function to check.
    :return: dictionary of parameter names to boolean indicating if they are pandas series.
    """
    new_signature = inspect.signature(hamilton_udf)
    new_sig_parameters = dict(new_signature.parameters)
    pandas_annotation = {
        name: param.annotation == pd.Series
        for name, param in new_sig_parameters.items()
        if param.default == inspect.Parameter.empty  # bound parameters will have a default value.
    }
    return pandas_annotation


def _bind_parameters_to_callable(
    actual_kwargs: dict,
    df_columns: Set[str],
    hamilton_udf: Callable,
    node_input_types: Dict[str, Tuple],
    node_name: str,
) -> Tuple[Callable, Dict[str, Any]]:
    """Function that we use to bind inputs to the function, or determine we should pull them from the dataframe.

    It does two things:

    1. If the parameter name matches a column name in the dataframe, create a pyspark column object for it.
    2. If the parameter name matches a key in the input dictionary, and the value is not a dataframe,\
    bind it to the function.

    :param actual_kwargs: the input dictionary of arguments for the function.
    :param df_columns: the set of column names in the dataframe.
    :param hamilton_udf: the callable to bind to.
    :param node_input_types: the input types of the function.
    :param node_name: name of the node/function.
    :return: a tuple of the callable and the dictionary of parameters to use for the callable.
    """
    params_from_df = {}
    for input_name in node_input_types.keys():
        if input_name in df_columns:
            params_from_df[input_name] = column(input_name)
        elif input_name in actual_kwargs and not isinstance(actual_kwargs[input_name], DataFrame):
            hamilton_udf = functools.partial(
                hamilton_udf, **{input_name: actual_kwargs[input_name]}
            )
        else:
            raise ValueError(
                f"Cannot satisfy {node_name} with input types {node_input_types} against a dataframe with "
                f"columns {df_columns} and input kwargs {actual_kwargs}."
            )
    return hamilton_udf, params_from_df


def _inspect_kwargs(kwargs: Dict[str, Any]) -> Tuple[DataFrame, Dict[str, Any]]:
    """Inspects kwargs, removes any dataframes, and returns the (presumed single) dataframe, with remaining kwargs.

    :param kwargs: the inputs to the function.
    :return: tuple of the dataframe and the remaining non-dataframe kwargs.
    """
    df = None
    actual_kwargs = {}
    for kwarg_key, kwarg_value in kwargs.items():
        if isinstance(kwarg_value, DataFrame):
            if df is None:
                df = kwarg_value
        else:
            actual_kwargs[kwarg_key] = kwarg_value
    return df, actual_kwargs


def _lambda_udf(
    df: DataFrame, node_: node.Node, hamilton_udf: Callable, actual_kwargs: Dict[str, Any]
) -> DataFrame:
    """Function to create a lambda UDF for a function.

    This functions does the following:

    1. Determines whether we can bind any arguments to the function, e.g. primitives.
    2. Determines what type of UDF it is, regular or Pandas, and processes the function accordingly.
    3. Determines the return type of the UDF.
    4. Creates the UDF and applies it to the dataframe.

    :param df: the spark dataframe to apply UDFs to.
    :param node_: the node representing the function.
    :param hamilton_udf: the function to apply.
    :param actual_kwargs: the actual arguments to the function.
    :return: the dataframe with one more column representing the result of the UDF.
    """
    hamilton_udf, params_from_df = _bind_parameters_to_callable(
        actual_kwargs, set(df.columns), hamilton_udf, node_.input_types, node_.name
    )
    pandas_annotation = _get_pandas_annotations(hamilton_udf)
    if any(pandas_annotation.values()) and not all(pandas_annotation.values()):
        raise ValueError(
            f"Currently unsupported function for {node_.name} with function signature:\n{node_.input_types}."
        )
    elif all(pandas_annotation.values()):
        # pull from annotation here instead of tag.
        base_type, type_args = htypes.get_type_information(node_.type)
        logger.debug("PandasUDF: %s, %s, %s", node_.name, base_type, type_args)
        if not type_args:
            raise ValueError(
                f"{node_.name} needs to be annotated with htypes.column[pd.Series, TYPE], "
                f"where TYPE could be the string name of the python type, or the python type itself."
            )
        type_arg = type_args[0]
        if isinstance(type_arg, str):
            spark_return_type = type_arg  # spark will handle converting it.
        else:
            spark_return_type = get_spark_type(actual_kwargs, df, hamilton_udf, type_arg)
        # remove because pyspark does not like extra function annotations
        hamilton_udf.__annotations__["return"] = base_type
        spark_udf = pandas_udf(hamilton_udf, spark_return_type)
    else:
        logger.debug("RegularUDF: %s, %s", node_.name, node_.type)
        spark_return_type = get_spark_type(actual_kwargs, df, hamilton_udf, node_.type)
        spark_udf = udf(hamilton_udf, spark_return_type)
    return df.withColumn(
        node_.name, spark_udf(*[_value for _name, _value in params_from_df.items()])
    )


class PySparkUDFGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    """UDF graph adapter for PySpark.

    This graph adapter enables one to write Hamilton functions that can be executed as UDFs in PySpark.

    Core to this is the mapping of function arguments to Spark columns available in the passed in dataframe.

    This adapter currently supports:

    - regular UDFs, these are executed in a row based fashion.
    - and a single variant of Pandas UDFs: func(series+) -> series
    - can also run regular Hamilton functions, which will execute spark driver side.

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self):
        self.df_object = None
        self.original_schema = []
        self.call_count = 0

    @staticmethod
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        """If the input is a pyspark dataframe, skip, else delegate the check."""
        if isinstance(input_value, DataFrame):
            return True
        return base.SimplePythonDataFrameGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        """Checks for the htype.column annotation and deals with it."""
        # Good Cases:
        # [pd.Series, int] -> [pd.Series, int]
        # pd.series -> pd.series
        # [pd.Series, int] -> int
        node_base_type, node_annotations = htypes.get_type_information(node_type)
        input_base_type, input_annotations = htypes.get_type_information(input_type)
        exact_match = node_type == input_type
        series_to_series = node_base_type == input_base_type
        if node_annotations:
            series_to_primitive = node_annotations[0] == input_base_type
        else:
            series_to_primitive = False
        return exact_match or series_to_series or series_to_primitive

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        """Given a node to execute, process it and apply a UDF if applicable.

        :param node: the node we're processing.
        :param kwargs: the inputs to the function.
        :return: the result of the function.
        """
        self.call_count += 1
        logger.debug("%s, %s", self.call_count, self.df_object)
        # get dataframe object out of kwargs
        df, actual_kwargs = _inspect_kwargs(kwargs)
        if df is None:  # there were no dataframes passed in. So regular function call.
            return node.callable(**actual_kwargs)
        if self.df_object is None:
            self.df_object = df  # this is done only once.
            self.original_schema = list(df.columns)
        logger.debug("%s, %s", self.call_count, self.df_object)
        logger.debug("%s, Before, %s", node.name, self.df_object.columns)
        schema_length = len(df.schema)
        df = _lambda_udf(self.df_object, node, node.callable, actual_kwargs)
        assert node.name in df.columns, f"Error {node.name} not in {df.columns}"
        delta = len(df.schema) - schema_length
        if delta == 0:
            raise ValueError(
                f"UDF {node.name} did not add any columns to the dataframe. "
                f"Does it already exist in the dataframe?"
            )
        self.df_object = df
        logger.debug("%s, After, %s", node.name, df.columns)
        return df

    def build_result(self, **outputs: Dict[str, Any]) -> DataFrame:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        df: DataFrame = self.df_object
        output_schema = self.original_schema
        # what's in the dataframe:
        for output_name, output_value in outputs.items():
            if output_name not in output_schema:
                output_schema.append(output_name)
            if output_name in df.columns:
                continue
            else:
                df = df.withColumn(output_name, lit(output_value))
        # original schema + new columns should be the order.
        # if someone requests a column that is in the original schema we won't duplicate it.
        result = df.select(*[column(col_name) for col_name in output_schema])
        # clear state out
        self.df_object = None
        self.original_schema = []
        return result
