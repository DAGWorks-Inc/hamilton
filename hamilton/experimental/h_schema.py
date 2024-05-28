import functools
import json
import logging
from pathlib import Path
from typing import Any, Dict, Literal, Union

import pyarrow
import pyarrow.ipc
from pyarrow.interchange import from_dataframe

from hamilton.experimental import h_databackends
from hamilton.graph_types import HamiltonGraph, HamiltonNode
from hamilton.lifecycle.api import GraphExecutionHook, NodeExecutionHook

logger = logging.getLogger(__file__)


@functools.singledispatch
def _get_arrow_schema(df, allow_copy: bool = True) -> pyarrow.Schema:
    # the property required for `from_dataframe()`
    if not hasattr(df, "__dataframe__"):
        raise NotImplementedError(f"Type {type(df)} is currently unsupported.")

    # try to convert to Pyarrow using zero-copy; otherwise hit RuntimeError
    try:
        df = from_dataframe(df, allow_copy=False)
    except RuntimeError as e:
        if allow_copy is False:
            raise e
        df = from_dataframe(df, allow_copy=True)

    return df.schema


@_get_arrow_schema.register
def _(
    df: Union[
        h_databackends.AbstractPandasDataFrame,
        h_databackends.AbstractGeoPandasDataFrame,
    ],
    **kwargs,
) -> pyarrow.Schema:
    table = pyarrow.Table.from_pandas(df)
    return table.schema


@_get_arrow_schema.register
def _(df: h_databackends.AbstractIbisDataFrame, **kwargs) -> pyarrow.Schema:
    return df.schema().to_pyarrow()


class SchemaValidator(NodeExecutionHook, GraphExecutionHook):
    """Collect dataframe and columns schemas at runtime. Can also conduct runtime checks against schemas"""

    def __init__(
        self,
        schema_dir: str = "./schema",
        check: bool = True,
        must_exist: bool = False,
        importance: Literal["warn", "fail"] = "warn",
    ):
        """
        :param schema_dir: Directory where schema files will be stored.
        :param check: If True, conduct schema validation
            Else generate schema files, potentially overwriting stored schemas.
        :param must_exist: If True when check=True, raise FileNotFoundError for missing schema files
            Else generate the missing schema files.
        :param importance: How to handle unequal schemas when check=True
            "warn": log a warning with the schema diff
            "fail": raise an exception with the schema diff
        """
        self.schemas: Dict[str, pyarrow.Schema] = {}
        self.reference_schemas: Dict[str, pyarrow.Schema] = {}
        self.schema_dir = schema_dir
        self.check = check
        self.must_exist = must_exist
        self.importance = importance
        self.h_graph: HamiltonGraph = None

    @staticmethod
    def get_dataframe_schema(node: HamiltonNode, df) -> pyarrow.Schema:
        """Get pyarrow schema of a table node result and add metadata to it."""
        schema = _get_arrow_schema(df)
        metadata = dict(
            name=str(node.name),
            documentation=str(node.documentation),
            version=str(node.version),
        )
        return schema.with_metadata(metadata)

    # TODO support column-level schema; could implement `column_to_dataframe` in h_databackend
    @staticmethod
    def get_column_schema(node: HamiltonNode, col) -> pyarrow.Schema:
        raise NotImplementedError

    def get_schema_path(self, node_name: str) -> Path:
        """Generate schema filepath based on node name.

        The `.schema` extension is arbitrary. Another common choice is `.arrow`
        but it wouldn't communicate that the file contains only schema metadata.
        The serialization format is IPC by default (see `.save_schema()`).
        """
        schema_file_suffix = ".schema"
        return Path(self.schema_dir, node_name).with_suffix(schema_file_suffix)

    def load_schema(self, node_name: str) -> pyarrow.Schema:
        """Load pyarrow schema from disk using IPC deserialization"""
        return pyarrow.ipc.read_schema(self.get_schema_path(node_name))

    def save_schema(self, node_name: str, schema: pyarrow.Schema) -> None:
        """Save pyarrow schema to disk using IPC serialization"""
        self.get_schema_path(node_name).write_bytes(schema.serialize())

    @staticmethod
    def diff_schemas(schema: pyarrow.Schema, reference: pyarrow.Schema, details: bool = False):
        # TODO expose schema diff when check fails
        raise NotImplementedError

    @staticmethod
    def human_readable_schema(schemas: Dict[str, pyarrow.Schema]) -> dict:
        """Give a human-readable representation of schema"""
        # TODO implement Hamilton metadata format and parser
        return {name: schema.to_string(truncate_metadata=False) for name, schema in schemas.items()}

    def handle_schema(self, node_name: str, schema: pyarrow.Schema) -> None:
        """Create or check schema based on __init__ parameters."""
        schema_path = self.get_schema_path(node_name)

        if self.check is False:
            self.save_schema(node_name, schema=schema)
            return

        if not schema_path.exists():
            if self.must_exist:
                raise FileNotFoundError(
                    f"{schema_path} not found. Set `check=False` or `must_exist=False` to create it."
                )
            else:
                self.save_schema(node_name, schema=schema)
                return

        reference_schema = self.load_schema(node_name)

        schema_error_message = f"Schemas for node `{node_name}` are unequal."
        # TODO expose `check_metadata` in equality
        schema_equality = schema.equals(reference_schema, check_metadata=False)
        if schema_equality is False:
            if self.importance == "warn":
                logger.warning(schema_error_message)
            elif self.importance == "fail":
                raise RuntimeError(schema_error_message)

    def run_before_graph_execution(
        self, *, graph: HamiltonGraph, inputs: Dict[str, Any], overrides: Dict[str, Any], **kwargs
    ):
        """Store schemas of inputs and overrides nodes that are tables or columns."""
        self.h_graph = (
            graph  # store HamiltonGraph to get HamiltonNodes in `run_after_node_execution`
        )
        for node_sets in [inputs, overrides]:
            if node_sets is None:
                continue

            for node_name, node_value in node_sets.items():
                h_node = self.h_graph[node_name]

                if isinstance(node_value, h_databackends.DATAFRAME_TYPES):
                    current_schema = self.get_dataframe_schema(h_node, node_value)
                    self.schemas[node_name] = current_schema
                    self.handle_schema(node_name=node_name, schema=current_schema)

    def run_after_node_execution(self, *, node_name: str, result: Any, **kwargs):
        """Store schema of executed node if table or column type."""
        h_node = self.h_graph[node_name]
        if isinstance(result, h_databackends.DATAFRAME_TYPES):
            current_schema = self.get_dataframe_schema(h_node, result)
            self.schemas[node_name] = current_schema
            self.handle_schema(node_name=node_name, schema=current_schema)

    def run_after_graph_execution(self, *args, **kwargs):
        """Store a human-readable JSON of all current schemas."""
        schemas_json_string = json.dumps(self.human_readable_schema(self.schemas))
        Path(f"{self.schema_dir}/schemas.json").write_text(schemas_json_string)

    def run_before_node_execution(self, *args, **kwargs):
        pass
