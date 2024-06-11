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
from hamilton.lifecycle import GraphExecutionHook, NodeExecutionHook

logger = logging.getLogger(__file__)


def pyarrow_schema_to_json(schema: pyarrow.Schema) -> dict:
    """Convert a pyarrow.Schema to a JSON-serializable dictionary

    Pyarrow provides a schema-to-string, but not schema-to-json.
    """
    schema_dict = dict(metadata={k.decode(): v.decode() for k, v in schema.metadata.items()})

    for name in schema.names:
        field = schema.field(name)
        schema_dict[str(name)] = dict(
            name=field.name,
            type=field.type.__str__(),  # __str__() and __repr__() of pyarrow.Field are different
            nullable=field.nullable,
            metadata=field.metadata,
        )

    return schema_dict


# TODO use ENUMs for a safe format and create a human readable format
# ENUM {0: "equal", 1: "added", 2: "removed", 3: "edited"}
def diff_schemas(
    current_schema: pyarrow.Schema,
    reference_schema: pyarrow.Schema,
    check_schema_metadata: bool = False,
    check_field_metadata: bool = False,
) -> dict:
    """Diff two Pyarrow schema field-by-field to return a human readable output.
    Options to diff schema and field metadata key-by-key.

    Returning an empty dict means equality / no diff
    """
    # if schemas are equal, return an empty diff
    if current_schema.equals(reference_schema, check_metadata=check_schema_metadata):
        return {}

    schema_diff = {}
    diff_template = "{ref} -> {cur}"  # template for human readable output

    # compare schema metadata
    if check_schema_metadata:
        schema_metadata_diff = {}
        d = dict_diff(current_schema.metadata, reference_schema.metadata)
        for key in d["added"]:
            schema_metadata_diff[key] = "added"
        for key in d["removed"]:
            schema_metadata_diff[key] = "deleted"
        for key in d["edited"]:
            schema_metadata_diff[key] = diff_template.format(
                ref=reference_schema.metadata[key], cur=current_schema.metadata[key]
            )

    # sets of column names in each schema
    current_names = set(current_schema.names)
    reference_names = set(reference_schema.names)

    # add to diffs field only present in either schema
    schema_diff.update(**{name: "added" for name in current_names.difference(reference_names)})
    schema_diff.update(**{name: "removed" for name in reference_names.difference(current_names)})
    # compare fields present in both schemas
    for name in current_names.intersection(reference_names):
        current_field = current_schema.field(name)
        reference_field = reference_schema.field(name)
        # if field is equal in both schema, skip it
        if current_field.equals(reference_field, check_metadata=check_field_metadata):
            continue

        # a pyarrow field has 4 attributes: name, type, nullable, metadata
        # name is equal
        field_diff = {}
        # check if type is equal
        if not current_field.type.equals(reference_field.type):
            field_diff["type"] = diff_template.format(
                ref=reference_field.type, cur=current_field.type
            )
        # check if nullable is equal
        if current_field.nullable != reference_field.nullable:
            field_diff["nullable"] = diff_template.format(
                ref=reference_field.nullable, cur=current_field.nullable
            )
        # check if metadata is equal
        if check_field_metadata:
            field_metadata_diff = {}
            d = dict_diff(current_field.metadata, reference_field.metadata)
            for key in d["added"]:
                field_metadata_diff[key] = "added"
            for key in d["removed"]:
                field_metadata_diff[key] = "deleted"
            for key in d["edited"]:
                field_metadata_diff[key] = diff_template.format(
                    ref=reference_field.metadata[key], cur=current_field.metadata[key]
                )

        schema_diff[name] = field_diff

    return schema_diff


def dict_diff(current_map: Dict[str, str], reference_map: Dict[str, str]) -> dict:
    """Compare two mappings and collect:
        - keys only in current_map
        - keys only in reference_map
        - keys in both, but with different values

    Returns a dictionary with a list for each category; empty dict means no diff
    """
    current_only, reference_only, edit = [], [], []

    for key, value1 in current_map.items():
        value2 = reference_map.get(key)
        if value2 is None:
            current_only.append(key)
            continue

        if value1 != value2:
            edit.append(key)

    for key, value2 in reference_map.items():
        value1 = current_map.get(key)
        if value1 is None:
            reference_only.append(key)
            continue

    return dict(added=current_only, removed=reference_only, edited=edit)


@functools.singledispatch
def _get_arrow_schema(df, allow_copy: bool = True) -> pyarrow.Schema:
    """Base case for getting a pyarrow schema from a dataframe.

    :param allow_copy: If True, allow to convert the object to Pyarrow
        even if zero-copy is unavailable

    It looks for the `__dataframe__` attribute associated with the dataframe interchange protocol
    ref: https://data-apis.org/dataframe-protocol/latest/API.html
    """
    if not hasattr(df, "__dataframe__"):
        # if hitting this condition, we can register a new function
        # to conver the dataframe to pyarrow
        raise NotImplementedError(f"Type {type(df)} is currently unsupported.")

    # try to convert to Pyarrow using zero-copy
    try:
        df = from_dataframe(df, allow_copy=False)
    # if unable to zero-copy, convert the object to Pyarrow
    # this may be undesirable if the object is large because of memory overhead
    except RuntimeError as e:
        if allow_copy is False:
            raise e
        df = from_dataframe(df, allow_copy=True)

    return df.schema


@_get_arrow_schema.register
def _(df: h_databackends.AbstractPandasDataFrame, **kwargs) -> pyarrow.Schema:
    """pandas to pyarrow using pyarrow-native method"""
    table = pyarrow.Table.from_pandas(df)
    return table.schema


@_get_arrow_schema.register
def _(df: h_databackends.AbstractIbisDataFrame, **kwargs) -> pyarrow.Schema:
    """Convert the Ibis schema to pyarrow Schema. The operation is lazy
    and doesn't require Ibis execution"""
    return df.schema().to_pyarrow()


# TODO lazy polars schema conversion
# ongoing polars discussion: https://github.com/pola-rs/polars/issues/15600


def get_dataframe_schema(df: h_databackends.DATAFRAME_TYPES, node: HamiltonNode) -> pyarrow.Schema:
    """Get pyarrow schema of a node result and store node metadata on the pyarrow schema."""
    schema = _get_arrow_schema(df)
    metadata = dict(
        name=str(node.name),
        documentation=str(node.documentation),
        version=str(node.version),
    )
    return schema.with_metadata(metadata)


def load_schema(path: Union[str, Path]) -> pyarrow.Schema:
    """Load pyarrow schema from disk using IPC deserialization"""
    return pyarrow.ipc.read_schema(path)


def save_schema(path: Union[str, Path], schema: pyarrow.Schema) -> None:
    """Save pyarrow schema to disk using IPC serialization"""
    Path(path).write_bytes(schema.serialize())


class SchemaValidator(NodeExecutionHook, GraphExecutionHook):
    """Collect dataframe and columns schemas at runtime. Can also conduct runtime checks against schemas"""

    def __init__(
        self,
        schema_dir: str = "./schemas",
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

        # create the directory where schemas will be stored
        Path(schema_dir).mkdir(parents=True, exist_ok=True)

    @property
    def json_schemas(self) -> Dict[str, dict]:
        """Return schemas collected during the run"""
        return {
            node_name: pyarrow_schema_to_json(schema) for node_name, schema in self.schemas.items()
        }

    # TODO support nodes returning columns by writing them as single column dataframe
    def get_schema_path(self, node_name: str) -> Path:
        """Generate schema filepath based on node name.

        The `.schema` extension is arbitrary. Another common choice is `.arrow`
        but it wouldn't communicate that the file contains only schema metadata.
        The serialization format is IPC by default (see `.save_schema()`).
        """
        return Path(self.schema_dir, node_name).with_suffix(".schema")

    def run_before_graph_execution(
        self, *, graph: HamiltonGraph, inputs: Dict[str, Any], overrides: Dict[str, Any], **kwargs
    ):
        """Store schemas of inputs and overrides nodes that are tables or columns."""
        self.h_graph = graph
        for node_sets in [inputs, overrides]:
            if node_sets is None:
                continue

            for node_name, node_value in node_sets.items():
                self.run_after_node_execution(node_name=node_name, result=node_value)

    def run_after_node_execution(self, *, node_name: str, result: Any, **kwargs):
        """Store schema of executed node if table or column type."""
        if not isinstance(result, h_databackends.DATAFRAME_TYPES):
            return

        # generate the schema from the HamiltonNode and node value
        node = self.h_graph[node_name]
        schema = get_dataframe_schema(df=result, node=node)
        self.schemas[node_name] = schema

        schema_path = self.get_schema_path(node_name)

        # behavior 1: only save schema
        if self.check is False:
            save_schema(self.get_schema_path(node_name), schema)
            return

        # behavior 2: handle missing reference schema while validating
        if not schema_path.exists():
            if self.must_exist:
                raise FileNotFoundError(
                    f"{schema_path} not found. Set `check=False` or `must_exist=False` to create it."
                )
            else:
                save_schema(self.get_schema_path(node_name), schema)
                return

        # behavior 3: validate current schema with reference schema
        reference_schema = load_schema(self.get_schema_path(node_name))
        # TODO expose `check_metadata` in equality
        schema_diff = diff_schemas(schema, reference_schema)
        if schema_diff != {}:
            if self.importance == "warn":
                logger.warning(schema_diff)
            elif self.importance == "fail":
                raise RuntimeError(schema_diff)

    def run_after_graph_execution(self, *args, **kwargs):
        """Store a human-readable JSON of all current schemas."""
        Path(f"{self.schema_dir}/schemas.json").write_text(json.dumps(self.json_schemas))

    def run_before_node_execution(self, *args, **kwargs):
        """Required by subclassing NodeExecutionHook"""
