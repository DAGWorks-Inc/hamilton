import pathlib

import pandas as pd

from hamilton.io.utils import SQL_METADATA, get_file_metadata, get_sql_metadata


def test_get_sql_metadata():
    results = 5
    table = "foo"
    query = "SELECT foo FROM bar"
    df = pd.DataFrame({"foo": ["bar"]})
    metadata1 = get_sql_metadata(table, df)[SQL_METADATA]
    metadata2 = get_sql_metadata(query, results)[SQL_METADATA]
    metadata3 = get_sql_metadata(query, "foo")[SQL_METADATA]
    assert metadata1["table_name"] == table
    assert metadata1["rows"] == 1
    assert metadata2["query"] == query
    assert metadata2["rows"] == 5
    assert metadata3["rows"] is None


def test_get_file_metadata(tmp_path: pathlib.Path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("test")
    metadata = get_file_metadata(file_path)
    assert metadata["file_metadata"]["path"] == str(file_path)
    assert metadata["file_metadata"]["size"] > 0
    assert metadata["file_metadata"]["last_modified"] == file_path.stat().st_mtime
    assert metadata["file_metadata"]["timestamp"] is not None


def test_get_file_metadata_url_schema():
    url = "s3://bucket/key"
    metadata = get_file_metadata(url)
    assert metadata["file_metadata"]["path"] == url
    assert metadata["file_metadata"]["scheme"] == "s3"
