import inspect
import typing

import polars as pl
from pyquokka.datastream import DataStream, GroupedDataStream

from hamilton import base, node


class QuokkaGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    def __init__(self, result_builder: base.ResultMixin = base.DictResult()):
        self.ds_objects = {}
        self.call_count = 0
        self.result_builder = result_builder

    def _lambda_udf(self, ds: DataStream, udf: typing.Callable) -> DataStream:
        """Function to wrap pulling metadata from a hamilton function"""
        sig = inspect.signature(udf)
        input_parameters = dict(sig.parameters)

        def hack_wrapper(x):
            # this is required because that's the signature datastream.with_column expects. i.e. a single arg function.
            kwargs = {k: x[k] for k in input_parameters}
            return udf(**kwargs)

        return ds.with_column(
            udf.__name__, hack_wrapper, required_columns=set(input_parameters.keys())
        )

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Very crappy code -- this is just to get something working. This should be refactored to be more generic."""

        if node.type == DataStream:
            print("got datastream", self.call_count, node.name)
            ds: DataStream = node.callable(**kwargs)
            if node.tags.get("materialize", "False") == "True":
                print("materializing", node.name)
                ds = ds.collect()
                self.call_count += 1
                return ds
        elif node.type == GroupedDataStream:
            print("got group by", self.call_count, node.name)
            ds = self.ds_objects["global"]
            print("before select", ds.schema)
            ds = ds.select(list(node.input_types.keys()))
            print("after select", ds.schema)
            group_by_cols = node.tags["group_by"].split(",")
            order_by_cols = node.tags["order_by"].split(",")
            ds: GroupedDataStream = ds.groupby(group_by_cols, orderby=order_by_cols)
            self.call_count += 1
            return ds
        elif node.type == pl.Series and len(node.input_types) == 1:
            # print('got extract_columns', self.call_count, node.name, kwargs)
            # get global one
            ds = self.ds_objects["global"]
        else:
            print("got udf", self.call_count, node.name, kwargs)
            ds: DataStream = self.ds_objects["global"]
            print(ds.schema)
            ds = self._lambda_udf(ds, node.callable)
        self.ds_objects["global"] = ds
        self.call_count += 1
        return ds

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> pl.DataFrame:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        # TODO: this is a bit hacky. `Collect()` should ideally be only called here.
        # Right now we assume that the final global object is what we need.
        # also this is brittle and would break if we request intermediate nodes for example.
        requested_ds_set = set(outputs.keys())
        ds = self.ds_objects["global"]
        if isinstance(ds, pl.DataFrame):
            df = ds[list(outputs.keys())]
        else:
            df: pl.DataFrame = ds.select(list(outputs.keys())).collect()
        global_ds_set = set(df.columns)
        if requested_ds_set.intersection(global_ds_set) != requested_ds_set:
            raise ValueError(
                f"Error: requested columns not found in final dataframe. "
                f"Missing: {requested_ds_set.difference(global_ds_set)}."
            )
        return df
