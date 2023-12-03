import os
from datetime import datetime
from typing import Any, Dict, Union

import pandas as pd

DATAFRAME_METADATA = "dataframe_metadata"
SQL_METADATA = "sql_metadata"
FILE_METADATA = "file_metadata"


def get_file_metadata(path: str) -> Dict[str, Any]:
    """Gives metadata from loading a file.

    Note: we reserve the right to change this schema. So if you're using this come
    chat so that we can make sure we don't break your code.

    This includes:
    - the file size
    - the file path
    - the last modified time
    - the current time
    """
    return {
        "size": os.path.getsize(path),
        "path": path,
        "last_modified": os.path.getmtime(path),
        "timestamp": datetime.now().utcnow().timestamp(),
    }


def get_dataframe_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """Gives metadata from loading a dataframe.

    Note: we reserve the right to change this schema. So if you're using this come
    chat so that we can make sure we don't break your code.

    This includes:
    - the number of rows
    - the number of columns
    - the column names
    - the data types
    """
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "datatypes": [str(t) for t in list(df.dtypes)],  # for serialization purposes
    }


def get_file_and_dataframe_metadata(path: str, df: pd.DataFrame) -> Dict[str, Any]:
    """Gives metadata from loading a file and a dataframe.

    Note: we reserve the right to change this schema. So if you're using this come
    chat so that we can make sure we don't break your code.

    This includes:
        file_meta:
            - the file size
            - the file path
            - the last modified time
            - the current time
        dataframe_meta:
        - the number of rows
        - the number of columns
        - the column names
        - the data types
    """
    return {FILE_METADATA: get_file_metadata(path), DATAFRAME_METADATA: get_dataframe_metadata(df)}


def get_sql_metadata(query_or_table: str, results: Union[int, pd.DataFrame]) -> Dict[str, Any]:
    """Gives metadata from reading a SQL table or writing to SQL db.

    Note: we reserve the right to change this schema. So if you're using this come
    chat so that we can make sure we don't break your code.

    This includes:
    - the number of rows read, added, or to add.
    - the sql query (e.g., "SELECT foo FROM bar")
    - the table name (e.g., "bar")
    - the current time
    """
    query = query_or_table if "SELECT" in query_or_table else None
    table_name = query_or_table if "SELECT" not in query_or_table else None
    if isinstance(results, int):
        rows = results
    elif isinstance(results, pd.DataFrame):
        rows = len(results)
    else:
        rows = None
    return {
        "rows": rows,
        "query": query,
        "table_name": table_name,
        "timestamp": datetime.now().utcnow().timestamp(),
    }
