import pandas as pd

from hamilton.io.utils import get_sql_metadata


def test_get_sql_metadata():
    results = 5
    table = "foo"
    query = "SELECT foo FROM bar"
    df = pd.DataFrame({"foo": ["bar"]})
    metadata1 = get_sql_metadata(table, df)
    metadata2 = get_sql_metadata(query, results)
    metadata3 = get_sql_metadata(query, "foo")
    assert metadata1["table_name"] == table
    assert metadata1["rows"] == 1
    assert metadata2["query"] == query
    assert metadata2["rows"] == 5
    assert metadata3["rows"] is None
