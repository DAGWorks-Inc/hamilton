import pandas as pd
import pytest

from hamilton.io.utils import get_df_metadata, get_sql_metadata


@pytest.fixture
def df() -> pd.DataFrame:
    yield pd.DataFrame({"foo": ["bar"]})


def test_get_sql_metadata(df: pd.DataFrame):
    query = "SELECT foo FROM bar"
    metadata1 = get_sql_metadata("foo", df)
    metadata2 = get_sql_metadata(query, 1)
    metadata3 = get_sql_metadata(query, "foo")

    assert metadata1["table_name"] == "foo"
    assert metadata1["rows"] == 1
    assert metadata2["query"] == query
    assert metadata2["rows"] == 1
    assert metadata3["rows"] is None


def test_get_df_metadata(df: pd.DataFrame) -> None:
    metadata = get_df_metadata(df)

    assert len(metadata) == 4
    assert metadata["rows"] == 1
    assert metadata["columns"] == 1
