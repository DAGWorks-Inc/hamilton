import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hamilton.function_modifiers import extract_columns, tag


# Data Loading
# Filtering is part of data loading -- do we also expose columns like this?
@extract_columns(
    *["l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus"]
)
def lineitem(
    sc: SparkSession,
    path: str,
    filter: str = "l_shipdate <= date '1998-12-01' - interval '90' day",
) -> pyspark.sql.DataFrame:
    """Loads and filters data from the lineitem table"""
    ds: pyspark.sql.DataFrame = (
        sc.read.option("inferSchema", True).option("header", True).csv(path, sep="|")
    )
    if filter:
        ds = ds.filter(filter)
    print(ds.schema)
    return ds


# transforms we want people to write
def disc_price(l_extendedprice: float, l_discount: float) -> float:
    """Computes the discounted price"""
    return l_extendedprice * (1 - l_discount)


def charge(l_extendedprice: float, l_discount: float, l_tax: float) -> float:
    """Computes the charge"""
    return l_extendedprice * (1 - l_discount) * (1 + l_tax)


# hacking things in via tags
@tag(group_by="l_returnflag,l_linestatus")
def grouped_lineitem(
    l_quantity: pyspark.sql.Column,
    l_extendedprice: pyspark.sql.Column,
    disc_price: pyspark.sql.Column,  # do we do some optional syntax here?
    # and at run time we check if the column exists, and if so use it. Else skip it.
    # basically it means that someone could write one function and determine if all are required or not.
    # and save them from having to update this function if they don't want a particular column being passed
    # through -- thought downstream of this, they would have to deal with it... so maybe not that valuable?
    charge: pyspark.sql.Column,
    l_discount: pyspark.sql.Column,
    l_returnflag: pyspark.sql.Column,
    l_linestatus: pyspark.sql.Column,
) -> pyspark.sql.GroupedData:
    """This function declares the "schema" the datastream needs to have via it's arguments.
    The body is blank because the graph adapter knows the actual logic to perform.

    Alternate syntax could be to have the decorator declare what's required, the function then takes the datastream, the
    group by happens in the function...
    """
    pass


# hack to get around https://github.com/marsupialtail/quokka/issues/23
# @tag(materialize="True")
def compute_aggregates(grouped_lineitem: pyspark.sql.GroupedData) -> pyspark.sql.DataFrame:
    # Thought: change these into individual functions?
    agg_map = {
        "l_quantity": ["sum", "avg"],
        "l_extendedprice": ["sum", "avg"],
        "disc_price": ["sum"],
        "charge": ["sum"],
        "l_discount": ["avg"],
        "*": ["count"],
    }
    agg_args = []
    for column_name, aggregates in agg_map.items():
        for aggregate in aggregates:
            func = getattr(F, aggregate)
            agg_args.append(func(F.column(column_name)))
    df = grouped_lineitem.agg(
        # {
        #     # "l_quantity": ["sum", "avg"],
        #     "l_quantity": "sum",
        #     # "l_extendedprice": ["sum", "avg"],
        #     "l_extendedprice": "avg",
        #     "disc_price": "sum",
        #     "charge": "sum",
        #     "l_discount": "avg",
        #     "*": "count",
        # }
        *agg_args
    )
    rename_map = {
        "l_returnflag": "al_returnflag",
        "l_linestatus": "al_linestatus",
        "count(1)": "row_count",
        "avg(l_quantity)": "l_quantity_mean",
        "sum(l_quantity)": "l_quantity_sum",
        "avg(l_extendedprice)": "l_extendedprice_mean",
        "sum(l_extendedprice)": "l_extendedprice_sum",
        "sum(disc_price)": "disc_price_sum",
        "sum(charge)": "charge_sum",
        "avg(l_discount)": "l_discount_mean",
    }
    for old_name, new_name in rename_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


# this doesn't seem like the right thing:
# def l_quantity_sum(grouped_lineitem: GroupedDataStream) -> pyspark.sql.Column:
#     pass


# hack to get around `@tag_outputs` not working with `@extract_columns` as expected.
@extract_columns(
    *[
        "row_count",
        "l_quantity_sum",
        "l_extendedprice_sum",
        "disc_price_sum",
        "charge_sum",
        "l_quantity_mean",
        "l_extendedprice_mean",
        "l_discount_mean",
        "al_returnflag",
        "al_linestatus",
    ]
)
def extract_aggregates(compute_aggregates: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return compute_aggregates
