import inspect
from typing import Any, Callable, Dict, Tuple, Type

from pyspark.sql import Column, DataFrame, GroupedData, types
from pyspark.sql.functions import column, udf

from hamilton import base, node


class PySparkGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    def __init__(self, result_builder: base.ResultMixin = base.DictResult()):
        self.df_objects = {}
        self.call_count = 0
        self.result_builder = result_builder

    @staticmethod
    def check_input_type(node_type: Type, input_value: Any) -> bool:
        return True

    @staticmethod
    def check_node_type_equivalence(node_type: Type, input_type: Type) -> bool:
        return True

    def _lambda_udf(self, df: DataFrame, hamilton_udf: Callable) -> DataFrame:
        sig = inspect.signature(hamilton_udf)
        input_parameters = dict(sig.parameters)
        return_type = sig.return_annotation
        print("lambda inputs", input_parameters, return_type, hamilton_udf.__name__)
        if return_type == float:
            spark_return_type = types.DoubleType()
        else:
            raise ValueError(f"Unsupported return type {return_type}")
        spark_udf = udf(hamilton_udf, spark_return_type)
        return df.withColumn(
            hamilton_udf.__name__, spark_udf(*[column(name) for name in sig.parameters.keys()])
        )

    def _sanitize_kwargs(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Sanitizes the kwargs to remove the datastream node name."""
        df_names = {}
        actual_kwargs = {}
        for kwarg_key, kwarg_value in kwargs.items():
            if isinstance(kwarg_value, dict) and "__df_name__" in kwarg_value:
                df_names[kwarg_key] = kwarg_value["__df_name__"]
                actual_kwargs[kwarg_key] = kwarg_value["__result__"]
            else:
                actual_kwargs[kwarg_key] = kwarg_value
        return actual_kwargs, df_names

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        self.call_count += 1
        actual_kwargs, df_names = self._sanitize_kwargs(kwargs)
        df_name_set = set(df_names.values())
        if node.type == DataFrame:
            # assumption is types into this function are only scalars, or other DataFrame/GroupedData objects
            df: DataFrame = node.callable(**actual_kwargs)
            self.df_objects[node.name] = df
            return {"__df_name__": node.name, "__result__": df}
        elif node.type == GroupedData:
            print("got group by", self.call_count, node.name)
            assert len(df_name_set) == 1, f"Error groupby got multiple DataFrames {df_names}"
            df = self.df_objects[df_names.popitem()[1]]
            print("before select", df.schema)
            df = df.select(list(node.input_types.keys()))
            print("after select", df.schema)
            group_by_cols = node.tags["group_by"].split(",")
            df: GroupedData = df.groupby(group_by_cols)
            self.df_objects[node.name] = df
            return {"__df_name__": node.name, "__result__": df}
        elif (
            node.type == Column
            and len(node.input_types) == 1
            and node.tags.get("__generated_by__", None) == "extract_columns"
        ):
            assert (
                len(df_name_set) == 1
            ), f"Error extract_columns got multiple DataFrames {df_names}"
            df_name = df_name_set.pop()
            print(node.name, node.tags)
            # print('got extract_columns', self.call_count, node.name, kwargs)
            # get global one
            df = self.df_objects[df_name]
            return {"__df_name__": df_name, "__result__": df}
        else:
            assert len(df_name_set) == 1, f"Error udf got multiple DataFrames {df_names}"
            print("got udf", self.call_count, node.name, kwargs)
            df_name = df_name_set.pop()
            df: DataFrame = self.df_objects[df_name]
            print(df.schema)
            df = self._lambda_udf(df, node.callable)
            self.df_objects[df_name] = df
            return {"__df_name__": df_name, "__result__": df}

    def build_result(self, **outputs: Dict[str, Any]) -> DataFrame:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        requested_ds_set = set(outputs.keys())
        actual_outputs, df_names = self._sanitize_kwargs(outputs)
        df_name_set = set(df_names.values())
        assert (
            len(df_name_set) == 1
        ), f"Error got multiple DataStreams to build result from {df_names}"
        df = self.df_objects[df_name_set.pop()]
        df = df.select(list(actual_outputs.keys()))
        global_ds_set = set(df.columns)
        if requested_ds_set.intersection(global_ds_set) != requested_ds_set:
            raise ValueError(
                f"Error: requested columns not found in final dataframe. "
                f"Missing: {requested_ds_set.difference(global_ds_set)}."
            )
        print("final schema", df.schema)
        return df
