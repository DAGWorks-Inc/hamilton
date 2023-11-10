# Purpose of this module

This module implements simple operations to interact with LanceDB.

# Configuration Options
This module doesn't receive any configuration.

## Inputs:
 - `schema`: To create a new table, you need to specified a pyarrow schema
 - `overwrite_table`: Allows you to overwrite existing table


# Limitations
- `push_data()` and `delete_data()` currently return the number of rows added and deleted, which requires reading the table in a Pyarrow table. This could impact performance if the table gets very large or push / delete are highly frequent.
