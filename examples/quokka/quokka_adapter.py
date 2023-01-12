import inspect
from typing import Any, Callable, Dict, Tuple

import polars as pl
from pyquokka.datastream import DataStream, GroupedDataStream

from hamilton import base, node


class QuokkaGraphAdapter(base.SimplePythonDataFrameGraphAdapter):
    def __init__(self, result_builder: base.ResultMixin = base.DictResult()):
        self.ds_objects = {}
        self.call_count = 0
        self.result_builder = result_builder  # not used...

    def _lambda_udf(self, ds: DataStream, udf: Callable) -> DataStream:
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

    def _sanitize_kwargs(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Sanitizes the kwargs to remove the datastream node name."""
        ds_names = {}
        actual_kwargs = {}
        for kwarg_key, kwarg_value in kwargs.items():
            if isinstance(kwarg_value, dict) and "__ds_name__" in kwarg_value:
                ds_names[kwarg_key] = kwarg_value["__ds_name__"]
                actual_kwargs[kwarg_key] = kwarg_value["__result__"]
            else:
                actual_kwargs[kwarg_key] = kwarg_value
        return actual_kwargs, ds_names

    def execute_node(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        self.call_count += 1
        actual_kwargs, ds_names = self._sanitize_kwargs(kwargs)
        ds_name_set = set(ds_names.values())
        if node.type == DataStream:
            if node.tags.get("operation", None) == "join":
                left = node.tags["left_on"]
                right = node.tags["right_on"]
                left_ds = self.ds_objects[ds_names[left]]
                right_ds = self.ds_objects[ds_names[right]]
                ds: DataStream = left_ds.join(
                    right_ds, left_on=left, right_on=right
                )  # only do inner for now.
                if node.tags.get("filter", None):
                    print(node.tags["filter"])
                    ds = ds.filter(node.tags["filter"])
                self.ds_objects[node.name] = ds
                return {"__ds_name__": node.name, "__result__": ds}
            else:
                # assumption is types into this function are only scalars, or other DataStream/GroupedDataStream objects
                ds: DataStream = node.callable(**actual_kwargs)
                self.ds_objects[node.name] = ds
                return {"__ds_name__": node.name, "__result__": ds}
        elif node.type == GroupedDataStream:
            print("got group by", self.call_count, node.name)
            assert len(ds_name_set) == 1, f"Error groupby got multiple DataStreams {ds_names}"
            ds = self.ds_objects[ds_names.popitem()[1]]
            print("before select", ds.schema)
            ds = ds.select(list(node.input_types.keys()))
            print("after select", ds.schema)
            group_by_cols = node.tags["group_by"].split(",")
            order_by_cols = node.tags["order_by"].split(",") if "order_by" in node.tags else None
            ds: GroupedDataStream = ds.groupby(group_by_cols, orderby=order_by_cols)
            self.ds_objects[node.name] = ds
            return {"__ds_name__": node.name, "__result__": ds}
        elif (
            node.type == pl.Series
            and len(node.input_types) == 1
            and node.tags.get("__generated_by__", None) == "extract_columns"
        ):
            assert (
                len(ds_name_set) == 1
            ), f"Error extract_columns got multiple DataStreams {ds_names}"
            ds_name = ds_name_set.pop()
            print(node.name, node.tags)
            # print('got extract_columns', self.call_count, node.name, kwargs)
            # get global one
            ds = self.ds_objects[ds_name]
            return {"__ds_name__": ds_name, "__result__": ds}
        else:
            assert len(ds_name_set) == 1, f"Error udf got multiple DataStreams {ds_names}"
            print("got udf", self.call_count, node.name, kwargs)
            ds_name = ds_name_set.pop()
            ds: DataStream = self.ds_objects[ds_name]
            print(ds.schema)
            ds = self._lambda_udf(ds, node.callable)
            self.ds_objects[ds_name] = ds
            return {"__ds_name__": ds_name, "__result__": ds}

    def execute_node2(self, node: node.Node, kwargs: Dict[str, Any]) -> Any:
        """Very crappy code -- this is just to get something working. This should be refactored to be more generic."""
        """
        Idea to handle multiple input data streams
         - DFS happens from outputs. So we should never have multiple DS's live as we progress through the DAG
         (assuming no parallel branches that never meet up together)
         - so what we need to keep track of is the name of the function that produced the datastream
          (do we pass it through kwargs as a hack?)
         - and who are the descendents of that function, so that they get the right datastream object.
         - things reset/are replaced when either a groupby, aggregation, or join happen.
        """
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

    def build_result(self, **outputs: Dict[str, Any]) -> pl.DataFrame:
        """Builds the result and brings it back to this running process.

        :param outputs: the dictionary of key -> Union[ray object reference | value]
        :return: The type of object returned by self.result_builder.
        """
        # TODO: this is a bit hacky. `Collect()` should ideally be only called here.
        # Right now we assume that the final global object is what we need.
        # also this is brittle and would break if we request intermediate nodes for example.
        requested_ds_set = set(outputs.keys())
        actual_outputs, ds_names = self._sanitize_kwargs(outputs)
        ds_name_set = set(ds_names.values())
        assert (
            len(ds_name_set) == 1
        ), f"Error got multiple DataStreams to build result from {ds_names}"
        ds = self.ds_objects[ds_name_set.pop()]
        if isinstance(ds, (DataStream, GroupedDataStream)):
            print(ds.explain(mode="text"))
            df = ds.collect()
        # if isinstance(ds, pl.DataFrame):
        df = df[list(actual_outputs.keys())]
        # else:
        # hack to get around issue with schema not being passed through correctly
        # df = ds.collect()
        # df: pl.DataFrame = ds.select(list(actual_outputs.keys())).collect()
        global_ds_set = set(df.columns)
        if requested_ds_set.intersection(global_ds_set) != requested_ds_set:
            raise ValueError(
                f"Error: requested columns not found in final dataframe. "
                f"Missing: {requested_ds_set.difference(global_ds_set)}."
            )
        return df
