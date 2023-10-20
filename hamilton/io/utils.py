import os
from datetime import datetime
from typing import Any, Dict, Union

import pandas as pd


def get_file_metadata(path: str) -> Dict[str, Any]:
    """Gives metadata from loading a file.
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


def get_sql_metadata(query_or_table: str, results: Union[int, pd.DataFrame]) -> Dict[str, Any]:
    """Gives metadata from reading a SQL table or writing to SQL db.
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
