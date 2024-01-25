import pandas as pd

from hamilton.io.utils import SQL_METADATA, get_sql_metadata


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
