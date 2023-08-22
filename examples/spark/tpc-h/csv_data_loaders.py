import os

import pyspark.sql as ps

from hamilton.function_modifiers import load_from, parameterize, source, value


@parameterize(
    customer_path={"suffix": value("customer.tbl")},
    lineitem_path={"suffix": value("lineitem.tbl")},
    nation_path={"suffix": value("nation.tbl")},
    orders_path={"suffix": value("orders.tbl")},
    part_path={"suffix": value("part.tbl")},
    partsupp_path={"suffix": value("partsupp.tbl")},
    region_path={"suffix": value("region.tbl")},
    supplier_path={"suffix": value("supplier.tbl")},
)
def paths(suffix: str, data_dir: str) -> str:
    return os.path.join(data_dir, suffix)


@load_from.csv(path=source("customer_path"), sep=value("|"), spark=source("spark"))
def customer(df: ps.DataFrame) -> ps.DataFrame:
    return df


# TODO -- parameterize these, but this is fine for now


@load_from.csv(path=source("lineitem_path"), sep=value("|"), spark=source("spark"))
def lineitem(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("nation_path"), sep=value("|"), spark=source("spark"))
def nation(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("orders_path"), sep=value("|"), spark=source("spark"))
def orders(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("part_path"), sep=value("|"), spark=source("spark"))
def part(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("partsupp_path"), sep=value("|"), spark=source("spark"))
def partsupp(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("region_path"), sep=value("|"), spark=source("spark"))
def region(df: ps.DataFrame) -> ps.DataFrame:
    return df


@load_from.csv(path=source("supplier_path"), sep=value("|"), spark=source("spark"))
def supplier(df: ps.DataFrame) -> ps.DataFrame:
    return df
