import polars as pl
from pyquokka.datastream import DataStream, GroupedDataStream
from pyquokka.df import QuokkaContext

from hamilton.function_modifiers import extract_columns, tag


# Data Loading
# Filtering is part of data loading -- do we also expose columns like this?
@extract_columns(
    *[
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_orderkey",
        "l_shipdate",
    ]
)
def lineitem_table(
    qc: QuokkaContext,
    path: str,
    lineitem_filter: str = None,
) -> DataStream:
    """Loads and filters data from the lineitem table"""
    ds: DataStream = qc.read_csv(path + "lineitem.tbl", sep="|", has_header=True)
    if lineitem_filter:
        ds = ds.filter(lineitem_filter)
    print(sorted(ds.schema))
    return ds


@extract_columns(*["o_orderdate", "o_orderkey", "o_shippriority", "o_custkey"])
def orders_table(
    qc: QuokkaContext,
    path: str,
    orders_filter: str = None,
) -> DataStream:
    """Loads and filters data from the orders table"""
    ds: DataStream = qc.read_csv(path + "orders.tbl", sep="|", has_header=True)
    if orders_filter:
        ds = ds.filter(orders_filter)
    print(sorted(ds.schema))
    return ds


@extract_columns(
    *[
        "c_custkey",
        "c_mktsegment",
    ]
)
def customer_table(
    qc: QuokkaContext,
    path: str,
    customer_filter: str = None,
) -> DataStream:
    """Loads and filters data from the customer table"""
    ds: DataStream = qc.read_csv(path + "customer.tbl", sep="|", has_header=True)
    if customer_filter:
        ds = ds.filter(customer_filter)
    print(sorted(ds.schema))
    return ds


# transforms we want people to write
def disc_price(l_extendedprice: pl.Series, l_discount: pl.Series) -> pl.Series:
    """Computes the discounted price"""
    return l_extendedprice * (1 - l_discount)


def charge(l_extendedprice: pl.Series, l_discount: pl.Series, l_tax: pl.Series) -> pl.Series:
    """Computes the charge"""
    return l_extendedprice * (1 - l_discount) * (1 + l_tax)


def high_orderpriority(o_orderpriority: pl.Series) -> pl.Series:
    return o_orderpriority == "1-URGENT" | o_orderpriority == "2-HIGH"


def low_orderpriority(o_orderpriority: pl.Series) -> pl.Series:
    return o_orderpriority != "1-URGENT" & o_orderpriority != "2-HIGH"


def clo_revenue(clo_extendedprice: pl.Series, clo_discount: pl.Series) -> pl.Series:
    return clo_extendedprice * (1 - clo_discount)


# hacking things in via tags
@tag(group_by="l_returnflag,l_linestatus", order_by="l_returnflag,l_linestatus")
def grouped_lineitem(
    l_quantity: pl.Series,
    l_extendedprice: pl.Series,
    disc_price: pl.Series,  # do we do some optional syntax here?
    # and at run time we check if the column exists, and if so use it. Else skip it.
    # basically it means that someone could write one function and determine if all are required or not.
    # and save them from having to update this function if they don't want a particular column being passed
    # through -- thought downstream of this, they would have to deal with it... so maybe not that valuable?
    charge: pl.Series,
    l_discount: pl.Series,
    l_returnflag: pl.Series,
    l_linestatus: pl.Series,
) -> GroupedDataStream:
    """This function declares the "schema" the datastream needs to have via it's arguments.
    The body is blank because the graph adapter knows the actual logic to perform.

    Alternate syntax could be to have the decorator declare what's required, the function then takes the datastream, the
    group by happens in the function...
    """
    pass


# hack to get around https://github.com/marsupialtail/quokka/issues/23
# @tag(materialize="True")
@extract_columns(
    *[
        "__count_sum",
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
def compute_aggregates(grouped_lineitem: GroupedDataStream) -> DataStream:
    # Thought: change these into individual functions?
    return grouped_lineitem.agg(
        {
            "l_quantity": ["sum", "avg"],
            "l_extendedprice": ["sum", "avg"],
            "disc_price": "sum",
            "charge": "sum",
            "l_discount": "avg",
            "*": "count",
        }
    ).rename({"l_returnflag": "al_returnflag", "l_linestatus": "al_linestatus"})


# this doesn't seem like the right thing:
# def l_quantity_sum(grouped_lineitem: GroupedDataStream) -> pl.Series:
#     pass


# hack to get around `@tag_outputs` not working with `@extract_columns` as expected.
# @extract_columns(
#     *[
#         "__count_sum",
#         "l_quantity_sum",
#         "l_extendedprice_sum",
#         "disc_price_sum",
#         "charge_sum",
#         "l_quantity_mean",
#         "l_extendedprice_mean",
#         "l_discount_mean",
#         "al_returnflag",
#         "al_linestatus",
#     ]
# )
# def extract_aggregates(compute_aggregates: DataStream) -> DataStream:
#     return compute_aggregates


# tpc-h 3
# on=None, left_on=None, right_on=None, suffix="_2", how="inner"
@tag(operation="join", left_on="l_orderkey", right_on="o_orderkey")
def lineitem_orders_table(
    l_discount: pl.Series,
    l_extendedprice: pl.Series,
    l_orderkey: pl.Series,  # what if there are two with same name? source__name syntax to disambiguate?
    l_shipdate: pl.Series,
    o_orderdate: pl.Series,
    o_orderkey: pl.Series,
    o_shippriority: pl.Series,
) -> DataStream:
    pass


@extract_columns(
    *[
        "lo_custkey",
        "lo_orderkey",
        "lo_orderdate",
        "lo_shippriority",
        "lo_shipdate",
        "lo_extendedprice",
        "lo_discount",
    ]
)
def extract_lineitem_orders_table(lineitem_orders_table: DataStream) -> DataStream:
    print(lineitem_orders_table.schema)
    renamed_schema = {
        col_name: f"lo_{'_'.join(col_name.split('_')[1:])}"
        for col_name in lineitem_orders_table.schema
    }
    return lineitem_orders_table.rename(renamed_schema)


# TODO: handle joining on datastream object.
@tag(
    operation="join",
    left_on="c_custkey",
    right_on="lo_custkey",
    # TODO handle filtering...?
    # fitler="c_mktsegment = 'BUILDING' and lo_orderdate < date '1995-03-15' and lo_shipdate > date '1995-03-15'"
)
def customer_lineitem_orders_table(
    c_custkey: pl.Series,
    c_mktsegment: pl.Series,
    lo_custkey: pl.Series,
    lo_orderdate: pl.Series,
    lo_orderkey: pl.Series,
    lo_shippriority: pl.Series,
    lo_shipdate: pl.Series,
) -> DataStream:
    pass


@extract_columns(
    *[
        "clo_custkey",
        "clo_orderkey",
        "clo_orderdate",
        "clo_shippriority",
        "clo_shipdate",
        "clo_extendedprice",
        "clo_discount",
    ]
)
def extract_customer_lineitem_orders_table(
    customer_lineitem_orders_table: DataStream,
) -> DataStream:
    print(customer_lineitem_orders_table.schema)
    renamed_schema = {
        col_name: f"clo_{'_'.join(col_name.split('_')[1:])}"
        for col_name in customer_lineitem_orders_table.schema
    }
    return customer_lineitem_orders_table.rename(renamed_schema)


@tag(group_by="clo_orderkey,clo_orderdate,clo_shippriority")
def grouped_customer_lineitem_orders_table(
    clo_orderkey: pl.Series,
    clo_orderdate: pl.Series,
    clo_shippriority: pl.Series,
    clo_revenue: pl.Series,
) -> GroupedDataStream:
    """This function declares the "schema" the datastream needs to have via it's arguments.
    The body is blank because the graph adapter knows the actual logic to perform.

    Alternate syntax could be to have the decorator declare what's required, the function then takes the datastream, the
    group by happens in the function...
    """
    pass


@extract_columns(*["aclo_revenue_sum", "aclo_orderkey", "aclo_orderdate", "aclo_shippriority"])
def aggregated_grouped_customer_lineitem_orders_table(
    grouped_customer_lineitem_orders_table: GroupedDataStream,
) -> DataStream:
    # Thought: change these into individual functions?
    return grouped_customer_lineitem_orders_table.agg({"clo_revenue": "sum"}).rename(
        {
            "clo_revenue_sum": "aclo_revenue_sum",
            "clo_orderkey": "aclo_orderkey",
            "clo_orderdate": "aclo_orderdate",
            "clo_shippriority": "aclo_shippriority",
        }
    )
