import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

from hamilton import contrib

with contrib.catch_import_errors(__name__, __file__, logger):
    import pyarrow as pa
    import lancedb

from hamilton.function_modifiers import tag


def vdb_client(uri: str = "./.lancedb") -> lancedb.DBConnection:
    return lancedb.connect(uri=uri)


@tag(side_effect="True")
def table_ref(
    vdb_client: lancedb.DBConnection,
    table_name: str,
    schema: pa.Schema,
    overwrite_table: bool = False,
) -> lancedb.db.LanceTable:
    """Create or reference a LanceDB table

    :param vdb_client: LanceDB connection.
    :param table_name: Name of the table.
    :param schema: Pyarrow schema defining the table schema.
    :param overwrite_table: If True, overwrite existing table
    :return: Reference to existing or newly created table.
    """

    try:
        table = vdb_client.open_table(table_name)
    except FileNotFoundError:
        mode = "overwrite" if overwrite_table else "create"
        table = vdb_client.create_table(name=table_name, schema=schema, mode=mode)

    return table


@tag(side_effect="True")
def reset_vdb(vdb_client: lancedb.DBConnection) -> Dict[str, List[str]]:
    """Drop all existing tables.

    :param vdb_client: LanceDB connection.
    :return: dictionary containing all the dropped tables.
    """
    tables_dropped = []
    for table_name in vdb_client.table_names():
        vdb_client.drop_table(table_name)
        tables_dropped.append(table_name)

    return dict(tables_dropped=tables_dropped)


@tag(side_effect="True")
def push_data(table_ref: lancedb.db.LanceTable, data: Any) -> Dict:
    """Push new data to the specified table.

    :param table_ref: Reference to the LanceDB table.
    :param data: Data to add to the table. Ref: https://lancedb.github.io/lancedb/guides/tables/#adding-to-a-table
    :return: Reference to the table and number of rows added
    """
    n_rows_before = table_ref.to_arrow().shape[0]
    table_ref.add(data)
    n_rows_after = table_ref.to_arrow().shape[0]
    n_rows_added = n_rows_after - n_rows_before
    return dict(table=table_ref, n_rows_added=n_rows_added)


@tag(side_effect="True")
def delete_data(table_ref: lancedb.db.LanceTable, delete_expression: str) -> Dict:
    """Delete existing data using an SQL expression.

    :param table_ref: Reference to the LanceDB table.
    :param data: Expression to select data. Ref: https://lancedb.github.io/lancedb/sql/
    :return: Reference to the table and number of rows deleted
    """
    n_rows_before = table_ref.to_arrow().shape[0]
    table_ref.delete(delete_expression)
    n_rows_after = table_ref.to_arrow().shape[0]
    n_rows_deleted = n_rows_before - n_rows_after
    return dict(table=table_ref, n_rows_deleted=n_rows_deleted)
