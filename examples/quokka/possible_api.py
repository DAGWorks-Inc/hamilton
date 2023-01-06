import polars as pl
from pyquokka.datastream import DataStream, GroupedDataStream
from pyquokka.df import QuokkaContext

from hamilton.function_modifiers import extract_columns, tag


# Data Loading
# Filtering is part of data loading -- do we also expose columns like this?
@extract_columns(
    *["l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus"]
)
def lineitem(
    qc: QuokkaContext,
    path: str,
    filter: str = "l_shipdate <= date '1998-12-01' - interval '90' day",
) -> DataStream:
    """Loads and filters data from the lineitem table"""
    ds: DataStream = qc.read_csv(path, sep="|", has_header=True)
    if filter:
        ds = ds.filter(filter)
    print(sorted(ds.schema))
    return ds


# transforms we want people to write
def disc_price(l_extendedprice: pl.Series, l_discount: pl.Series) -> pl.Series:
    """Computes the discounted price"""
    return l_extendedprice * (1 - l_discount)


def charge(l_extendedprice: pl.Series, l_discount: pl.Series, l_tax: pl.Series) -> pl.Series:
    """Computes the charge"""
    return l_extendedprice * (1 - l_discount) * (1 + l_tax)


# hacking things in via tags
@tag(group_by="l_returnflag,l_linestatus", order_by="l_returnflag,l_linestatus")
def grouped_lineitem(
    l_quantity: pl.Series,
    l_extendedprice: pl.Series,
    disc_price: pl.Series,
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
@tag(materialize="True")
def compute_aggregates(grouped_lineitem: GroupedDataStream) -> DataStream:
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


# hack to get around `@tag_outputs` not working with `@extract_columns` as expected.
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
def extract_aggregates(compute_aggregates: DataStream) -> DataStream:
    return compute_aggregates
