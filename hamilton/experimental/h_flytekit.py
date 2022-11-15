import logging
import typing
from pathlib import Path
from typing import Type

import flytekit
import pandas as pd
from flytekit import FlyteContext, FlyteContextManager, Literal, LiteralType
from flytekit.core import promise, workflow
from flytekit.core.type_engine import T, TypeEngine, TypeTransformer
from flytekit.models.literals import Scalar, Schema
from flytekit.models.types import SchemaType
from flytekit.types.schema import SchemaEngine, SchemaFormat, SchemaHandler
from flytekit.types.schema.types_pandas import (
    PandasDataFrameTransformer,
    PandasSchemaReader,
    PandasSchemaWriter,
)

from hamilton import base, node

logger = logging.getLogger(__name__)


class PandasSeriesTransformer(TypeTransformer[pd.Series]):
    """
    Creates a transformer to handle PandasSeries, Similar to PandasTransformer
    in flight repo https://github.com/flyteorg/flytekit/blob/master/flytekit/types/schema/types_pandas.py
    In essence, we convert the series to dataframe before writing to the disk.
    """

    def __init__(self):
        super().__init__("PandasSeries<->GenericSchema", pd.Series)

    def get_literal_type(self, t: Type[pd.Series]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type())

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pd.Series,
        python_type: Type[pd.Series],
        expected: LiteralType,
    ) -> Literal:
        local_dir = ctx.file_access.get_random_local_directory()
        w = PandasSchemaWriter(local_dir=Path(local_dir), cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val.to_frame(name=str(python_val.name)))
        remote_path = ctx.file_access.get_random_remote_directory()
        ctx.file_access.put_data(local_dir, remote_path, is_multipart=True)
        return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type())))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pd.Series]
    ) -> T:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pd.Series()
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(lv.scalar.schema.uri, local_dir, is_multipart=True)
        r = PandasSchemaReader(local_dir=Path(local_dir), cols=None, fmt=SchemaFormat.PARQUET)
        # return first column as series should have only one
        return r.all().iloc[:, 0]

    def to_html(
        self, ctx: FlyteContext, python_val: pd.Series, expected_python_type: Type[pd.Series]
    ) -> str:
        return python_val.describe().to_string()

    def _get_schema_type(self):
        return SchemaType(columns=[])


# Register with FlyteKit type engine
TypeEngine.register(PandasSeriesTransformer())


def _check_supported_type_in_flyte(node_type: typing.Type) -> bool:
    try:
        TypeEngine.get_transformer(node_type)
        return True
    except ValueError:
        return False


class FlyteKitAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's require to run Hamilton on Flytekit

    Use `pip install sf-hamilton[flytekit] to get the dependencies required to run this.

    Flytekit Python is the Python Library for easily authoring, testing, deploying,
    and interacting with Flyte tasks, workflows, and launch plans

    If you have custom types that are not supported with FlyteKit that you want to use in Hamilton,
    you can register them with the type engine in the driver.

    Dataclasses, numpy arrays, pandas series and python native types are supported OOTB.


    """

    def __init__(
        self, workflow_name: str, result_builder: base.ResultMixin = base.PandasDataFrameResult
    ):
        """Constructor"""
        self.workflow = workflow.ImperativeWorkflow(workflow_name)
        self.result_builder = result_builder

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        if isinstance(node_type, promise.Promise):
            return True
        return node_type == typing.Any or isinstance(input_value, node_type)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph
        :param kwargs: the arguments that should be passed to it
        :return: Create a flytekit task and execute it?
        """
        # define a python task
        task = flytekit.PythonFunctionTask(task_config=None, task_function=node.callable)
        input_kwargs = {}
        # map node inputs to flyte inputs
        for input_node in node.dependencies:
            # avoids adding the same input twice
            if input_node.name not in self.workflow.inputs:
                if _check_supported_type_in_flyte(input_node.type):
                    self.workflow.add_workflow_input(input_node.name, input_node.type)
                else:
                    raise ValueError(
                        f"Input type {input_node.type} is not supported by Flytekit. "
                        f"Please register it with the type engine in the driver code."
                    )
            input_kwargs[input_node.name] = self.workflow.inputs[input_node.name]
        # add the node to workflow, Adding the entity i.e. the callable to the workflow also populates the nodes outputs
        wf_node = self.workflow.add_entity(task, **input_kwargs)
        # map node output to flyte output
        if wf_node.outputs is not None:
            # we should have single output....
            output_name, output_value = next(iter(wf_node.outputs.items()))
            # map node output to flyte output
            if _check_supported_type_in_flyte(node.type):
                self.workflow.add_workflow_output(node.name, output_value, node.type)
            else:
                raise ValueError(
                    f"Output type {node.type} is not supported by Flytekit. "
                    f"Please register it with the type engine in the driver code."
                )

        return wf_node

    def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        execution_results = self.workflow.execute(**outputs)
        if execution_results is not None:
            expected_output_names = list(self.workflow.python_interface.outputs.keys())
            if len(expected_output_names) == 1:
                wf_outputs_as_map = {expected_output_names[0]: execution_results}
            else:
                wf_outputs_as_map = {
                    expected_output_names[i]: execution_results[i]
                    for i, _ in enumerate(execution_results)
                }
            for k, v in wf_outputs_as_map.items():
                outputs[k] = v
        return self.result_builder.build_result(**outputs)
