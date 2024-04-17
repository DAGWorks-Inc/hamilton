import sqlglot


def parse_sql_query(query: str) -> dict:
    """Parses a sql query and returns a string that can be used as a filename.

    TODO: figure out best long term place for this.

    :param query: The query to parse.
    :return: metadata about the query
    """
    parsed = sqlglot.parse_one(query)
    metadata = {}
    for idx, table in enumerate(parsed.find_all(sqlglot.exp.Table)):
        if not table.catalog or not table.db:
            continue
        metadata[f"table-{idx}"] = {
            "catalog": table.catalog,
            "database": table.db,
            "name": table.name,
        }
    return metadata
