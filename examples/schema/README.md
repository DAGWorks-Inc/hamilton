# Schema tracking and validation

This examples showcases the `SchemaValidator()` adapter. At runtime, it will collect the schema of DataFrame-like objects and stored them as `pyarrow.Schema` objects. On subsequent runs, it will check if the schema of a noed output matches the stored reference schema.

> NOTE­. `SchemaValidator` is currently under `hamilton.experimental.h_schema`. We're currently iterating over the API and are looking forward to integrate it in the main library.

## Content
- `dataflow.py` defines a dataflow with 3 nodes returning pyarrow, ibis, and pandas dataframes and use the `SchemaValidator()` to track their schema. After running it once, you can change the `Driver` config to use `version="2"` and see the schema validation fail.
- `/my_schemas` includes the stored `.schema` files. They are created using the standard IPC Arrow serialization format. Unfortunately, they are not human readable. We produce a readable `schemas.json` with all the original schema and metadata. We'll be improving it's readability over time.

You can play with the arguments `--version [1, 2, 3]` and `--no-check` to update the stored schema, trigger failing schema checks, and view the diffs.
