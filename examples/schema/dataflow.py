import duckdb
import ibis
import ibis.expr.types as ir
import pandas as pd
import pyarrow

from hamilton.function_modifiers import config


def pyarrow_table() -> pyarrow.Table:
    """Create a duckdb table in-memory and return it as a PyArrow table"""
    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)")
    con.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
    return con.execute("SELECT * FROM items").fetch_arrow_table()


def ibis_rename(pyarrow_table: pyarrow.Table) -> ir.Table:
    """Rename the columns"""
    table = ibis.memtable(pyarrow_table)
    return table.rename(object="item", price="value", number="count")


@config.when(version="1")
def pandas_new_col__v1(ibis_rename: ir.Table) -> pd.DataFrame:
    """Add the column `new_col`"""
    df = ibis_rename.to_pandas()
    df["new_col"] = True
    return df


@config.when(version="2")
def pandas_new_col__v2(ibis_rename: ir.Table) -> pd.DataFrame:
    """Add the column `new_col`"""
    df = ibis_rename.to_pandas()
    df["another_col"] = "X"
    return df


if __name__ == "__main__":
    import __main__

    from hamilton import driver
    from hamilton.experimental import h_schema

    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_config(dict(version="2"))
        .with_adapters(h_schema.SchemaValidator("./my_schemas", importance="fail"))
        .build()
    )
    res = dr.execute(["pandas_new_col"])
    print(res["pandas_new_col"].head())

    collected_schemas = dr.adapter.adapters[0].schemas
    for node_name, schema in collected_schemas.items():
        print()
        print(f"## {node_name}")
        print(schema)
