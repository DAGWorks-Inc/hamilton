"""The graph adapter to make things work."""
import functools
import inspect
from typing import Any, Callable, Dict, Tuple, Type, Union

import pandas as pd
from pyspark.sql import DataFrame, GroupedData, types
from pyspark.sql.functions import column, lit, pandas_udf, udf

from hamilton import base, node


class PySparkGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    def __init__(self):
        self.df_object = None
        self.original_schema = []
        self.call_count = 0

    @staticmethod
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        if isinstance(input_value, DataFrame):
            return True
        return base.SimplePythonDataFrameGraphAdapter.check_input_type(node_type, input_value)

    def _lambda_udf(
        self, df: DataFrame, node_: node.Node, hamilton_udf: Callable, actual_kwargs: Dict[str, Any]
    ) -> DataFrame:
        sig = inspect.signature(hamilton_udf)
        input_parameters = dict(sig.parameters)
        return_type = sig.return_annotation
        params_from_df = {}

        columns_present = set(df.columns)
        for input_name in input_parameters.keys():
            if input_name in columns_present:
                params_from_df[input_name] = column(input_name)
                continue
            elif input_name in actual_kwargs and not isinstance(
                actual_kwargs[input_name], DataFrame
            ):
                hamilton_udf = functools.partial(
                    hamilton_udf, **{input_name: actual_kwargs[input_name]}
                )
            else:
                raise ValueError(
                    f"Cannot satisfy {hamilton_udf.__name__} with columns {columns_present}, "
                    f"and kwargs {actual_kwargs} not in kwargs"
                )
        sig2 = inspect.signature(hamilton_udf)
        input_parameters2 = dict(sig2.parameters)
        pandas_annotation = {
            name: param.annotation == pd.Series
            for name, param in input_parameters2.items()
            if param.default == inspect.Parameter.empty
        }
        if any(pandas_annotation.values()) and not all(pandas_annotation.values()):
            print(inspect.signature(hamilton_udf), actual_kwargs)
            raise ValueError(
                f"Currently unsupported function or did not get passed literal values:\n{sig}."
            )
        elif all(pandas_annotation.values()):
            spark_return_type = node_.tags.get("return_type", None)
            if spark_return_type is None:
                raise ValueError(
                    f"{node_.name} needs to specify a 'return_type' tag with the string name to be used as a pandas_udf."
                )
            spark_udf = pandas_udf(hamilton_udf, spark_return_type)
        else:
            if return_type == float:
                spark_return_type = types.DoubleType()
            elif return_type == int:
                spark_return_type = types.IntegerType()
            elif return_type == str:
                spark_return_type = types.StringType()
            elif return_type == bool:
                spark_return_type = types.BooleanType()
            else:
                print(inspect.signature(hamilton_udf), actual_kwargs, df.columns)
                raise ValueError(
                    f"Currently unsupported return type {return_type}. "
                    f"Please create an issue or PR to add support for this type."
                )
            spark_udf = udf(hamilton_udf, spark_return_type)
        return df.withColumn(
            node_.name, spark_udf(*[_value for _name, _value in params_from_df.items()])
        )

    def _check_kwargs(
        self, kwargs: Dict[str, Any]
    ) -> Tuple[Union[DataFrame, GroupedData], Dict[str, Any]]:
        """Sanitizes the kwargs to remove the dataframes"""
        df = None
        actual_kwargs = {}
        for kwarg_key, kwarg_value in kwargs.items():
            if isinstance(kwarg_value, (DataFrame, GroupedData)):
                if df is None:
                    df = kwarg_value
            else:
                actual_kwargs[kwarg_key] = kwarg_value
        return df, actual_kwargs

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        self.call_count += 1
        print(self.call_count, self.df_object)
        # get dataframe object out of kwargs
        df, actual_kwargs = self._check_kwargs(kwargs)
        if df is None:  # there were no dataframes passed in. So regular function call.
            return node.callable(**actual_kwargs)
        if self.df_object is None:
            self.df_object = df
            self.original_schema = list(df.columns)
        print(self.call_count, self.df_object)
        print(node.name, "Before", self.df_object.columns)
        schema_length = len(df.schema)
        df = self._lambda_udf(self.df_object, node, node.callable, actual_kwargs)
        assert node.name in df.columns, f"Error {node.name} not in {df.columns}"
        delta = len(df.schema) - schema_length
        if delta == 0:
            raise ValueError(f"UDF {node.name} did not add any columns to the dataframe")
        self.df_object = df
        print(node.name, "After", df.columns)
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
        # we only return DFs
        result = df.select(*[column(col_name) for col_name in output_schema])
        # clear state out
        self.df_object = None
        self.original_schema = []
        return result
