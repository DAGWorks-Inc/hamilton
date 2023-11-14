# Purpose of this module

This module implements vector and full-text search using LanceDB.

# Configuration Options
This module doesn't receive any configuration.

## Inputs:
 - `url`: The url to the local LanceDB instance.
 - `table_name`: The name of the table to interact with.
 - `schema`: To create a new table, you need to specify a pyarrow schema.
 - `vector_query`: The embedding vector of the text query.
 - `full_text_query`: The text content to search for in the columns `full_text_index`.

# Limitations
- Full-text search needs to rebuild the index to include newly added data. By default `rebuild_index=True` will rebuild the index on each call to `full_text_search()` for safety. Pass `rebuild_index=False` when making multiple search queries without adding new data.
- `insert()` and `delete()` returns the number of rows added and deleted, which requires reading the table in a Pyarrow table. This could impact performance if the table gets very large or push / delete are highly frequent.
