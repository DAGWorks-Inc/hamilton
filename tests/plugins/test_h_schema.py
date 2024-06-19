import json
import pathlib

import pyarrow
import pytest

from hamilton.plugins import h_schema


@pytest.fixture
def schema1():
    yield pyarrow.schema(
        [
            ("foo", pyarrow.string()),
            ("bar", pyarrow.int64()),
        ]
    )


@pytest.fixture
def schema2():
    yield pyarrow.schema(
        [
            ("foo", pyarrow.string()),
            ("bar", pyarrow.int64()),
            ("baz", pyarrow.bool_()),
        ]
    )


@pytest.fixture
def schema3():
    yield pyarrow.schema(
        [
            ("foo", pyarrow.string()),
            ("bar", pyarrow.float64()),
        ]
    )


@pytest.fixture
def metadata1():
    yield {"key": "value1"}


@pytest.fixture
def metadata2():
    yield {"key": "value2"}


def test_schema_no_diff(schema1: pyarrow.Schema):
    diff = h_schema.diff_schemas(schema1, schema1)
    assert diff == {}

    human_readable_diff = h_schema.human_readable_diff(diff)
    assert human_readable_diff == {}


def test_schema_added_node(schema1: pyarrow.Schema, schema2: pyarrow.Schema):
    schema_diff = h_schema.diff_schemas(schema2, schema1)
    assert schema_diff["baz"].diff == h_schema.Diff.ADDED

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {"baz": "+"}


def test_schema_removed_node(schema1: pyarrow.Schema, schema2: pyarrow.Schema):
    schema_diff = h_schema.diff_schemas(schema1, schema2)
    assert schema_diff["baz"].diff == h_schema.Diff.REMOVED

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {"baz": "-"}


def test_schema_edited_node(schema1: pyarrow.Schema, schema3: pyarrow.Schema):
    schema_diff = h_schema.diff_schemas(schema1, schema3)
    assert schema_diff["bar"].diff == h_schema.Diff.UNEQUAL

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {"bar": {"type": {"cur": "int64", "ref": "double"}}}


def test_schema_equal_no_schema_metadata_diff(schema1: pyarrow.Schema):
    metadata = {"key": "value"}
    schema1 = schema1.with_metadata(metadata)
    schema_diff = h_schema.diff_schemas(schema1, schema1, check_schema_metadata=True)
    assert schema_diff == {}

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {}


def test_schema_unequal_but_no_schema_metadata_diff(
    schema1: pyarrow.Schema,
    schema2: pyarrow.Schema,
    metadata1: dict,
):
    schema1 = schema1.with_metadata(metadata1)
    schema2 = schema2.with_metadata(metadata1)

    schema_diff = h_schema.diff_schemas(schema1, schema2, check_schema_metadata=True)

    assert schema_diff["baz"].diff == h_schema.Diff.REMOVED
    assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.EQUAL

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {"baz": "-"}


def test_schema_added_schema_metadata(schema1: pyarrow.Schema, metadata1: dict):
    schema1_with_metadata = schema1.with_metadata(metadata1)

    schema_diff = h_schema.diff_schemas(schema1_with_metadata, schema1, check_schema_metadata=True)

    assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.ADDED

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {h_schema.SCHEMA_METADATA_FIELD: {"key": "+"}}


def test_schema_removed_schema_metadata(schema1: pyarrow.Schema, metadata1: dict):
    schema1_with_metadata = schema1.with_metadata(metadata1)

    schema_diff = h_schema.diff_schemas(schema1, schema1_with_metadata, check_schema_metadata=True)

    assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.REMOVED

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {h_schema.SCHEMA_METADATA_FIELD: {"key": "-"}}


def test_schema_edited_schema_metadata(schema1: pyarrow.Schema, metadata1: dict, metadata2: dict):
    schema1_with_metadata1 = schema1.with_metadata(metadata1)
    schema1_with_metadata2 = schema1.with_metadata(metadata2)

    schema_diff = h_schema.diff_schemas(
        schema1_with_metadata1, schema1_with_metadata2, check_schema_metadata=True
    )

    assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.UNEQUAL

    human_readable_diff = h_schema.human_readable_diff(schema_diff)
    assert human_readable_diff == {
        h_schema.SCHEMA_METADATA_FIELD: {"key": {"cur": "value1", "ref": "value2"}}
    }


# def test_schema_added_field_metadata(schema1: pyarrow.Schema, metadata1: dict):
#     field1 = schema1.field(1)
#     field1_with_metadata = field1.with_metadata(metadata1)
#     schema1.set(1, field1_with_metadata)

#     schema_diff = h_schema.diff_schemas(schema1, schema1, check_field_metadata=True)
#     print(schema_diff)

#     assert schema_diff[field1.name]["metadata"].diff == h_schema.Diff.ADDED

#     human_readable_diff = h_schema.human_readable_diff_json(schema_diff)
#     assert human_readable_diff == {h_schema.SCHEMA_METADATA_FIELD: {"key": "+"}}


# def test_schema_removed_field_metadata(schema1: pyarrow.Schema, metadata1: dict):
#     schema1_with_metadata = schema1.with_metadata(metadata1)

#     schema_diff = h_schema.diff_schemas(schema1, schema1_with_metadata, check_field_metadata=True)

#     assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.REMOVED

#     human_readable_diff = h_schema.human_readable_diff_json(schema_diff)
#     assert human_readable_diff == {h_schema.SCHEMA_METADATA_FIELD: {"key": "-"}}


# def test_schema_edited_field_metadata(schema1: pyarrow.Schema, metadata1: dict, metadata2: dict):
#     schema1_with_metadata1 = schema1.with_metadata(metadata1)
#     schema1_with_metadata2 = schema1.with_metadata(metadata2)

#     schema_diff = h_schema.diff_schemas(
#         schema1_with_metadata1,
#         schema1_with_metadata2,
#         check_field_metadata=True
#     )

#     assert schema_diff[h_schema.SCHEMA_METADATA_FIELD]["key"].diff == h_schema.Diff.UNEQUAL

#     human_readable_diff = h_schema.human_readable_diff_json(schema_diff)
#     assert human_readable_diff == {h_schema.SCHEMA_METADATA_FIELD: {'key': {"cur": "value1", "ref": "value2"}}}


def test_pyarrow_schema_equals_json_schema(schema1: pyarrow.Schema, metadata1: dict):
    schema1_with_metadata1 = schema1.with_metadata(metadata1)
    expected_json = {
        h_schema.SCHEMA_METADATA_FIELD: {"key": "value1"},
        "foo": {"name": "foo", "type": "string", "nullable": True, "metadata": {}},
        "bar": {"name": "bar", "type": "int64", "nullable": True, "metadata": {}},
    }

    schema_json = h_schema.pyarrow_schema_to_json(schema1_with_metadata1)
    assert expected_json == schema_json
    # ensure the returned schema is JSON-serializable
    assert json.dumps(expected_json) == json.dumps(schema_json)


def test_load_schema_from_disk(schema1: pyarrow.Schema, tmp_path: pathlib.Path):
    schema_path = tmp_path / "my_schema.schema"
    pathlib.Path(schema_path).write_bytes(schema1.serialize())
    loaded_schema = h_schema.load_schema(schema_path)
    assert schema1.equals(loaded_schema)


def test_save_schema_to_disk(schema1: pyarrow.Schema, tmp_path: pathlib.Path):
    schema_path = tmp_path / "my_schema.schema"
    h_schema.save_schema(path=schema_path, schema=schema1)
    loaded_schema = pyarrow.ipc.read_schema(schema_path)
    assert schema1.equals(loaded_schema)
