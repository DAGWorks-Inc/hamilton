"""Shows how you can use Hamilton with Quokka today.

There isn't any transform re-use due to relying on mutation order
of the central DataStream object.
"""
from pyquokka.datastream import DataStream, GroupedDataStream
from pyquokka.df import QuokkaContext


def lineitem(qc: QuokkaContext, path: str) -> DataStream:
    """Loads and filters data from the lineitem table"""
    ds = qc.read_csv(path + "lineitem.tbl", sep="|", has_header=True)
    return ds.filter("l_shipdate <= date '1998-12-01' - interval '90' day")


def lineitem_mutation_1(lineitem: DataStream) -> DataStream:
    return lineitem.with_column(
        "disc_price",
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]),
        required_columns={"l_extendedprice", "l_discount"},
    )


def lineitem_mutation_2(lineitem_mutation_1: DataStream) -> DataStream:
    return lineitem_mutation_1.with_column(
        "charge",
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) * (1 + x["l_tax"]),
        required_columns={"l_extendedprice", "l_discount", "l_tax"},
    )


def grouped_lineitem(lineitem_mutation_2: DataStream) -> GroupedDataStream:
    return lineitem_mutation_2.groupby(
        ["l_returnflag", "l_linestatus"], orderby=["l_returnflag", "l_linestatus"]
    )


def computed_aggregates(grouped_lineitem: GroupedDataStream) -> DataStream:
    return grouped_lineitem.agg(
        {
            "l_quantity": ["sum", "avg"],
            "l_extendedprice": ["sum", "avg"],
            "disc_price": "sum",
            "charge": "sum",
            "l_discount": "avg",
            "*": "count",
        }
    )


def manual_hello_world_hamilton(qc: QuokkaContext, path: str):
    d = lineitem(qc, path)
    d = lineitem_mutation_1(d)
    d = lineitem_mutation_2(d)
    f = grouped_lineitem(d)
    df = computed_aggregates(f)
    return df.collect()


def hello_world_hamilton(qc: QuokkaContext, path: str):
    from hamilton import ad_hoc_utils, base, driver

    temp_module = ad_hoc_utils.create_temporary_module(
        lineitem, lineitem_mutation_1, lineitem_mutation_2, grouped_lineitem, computed_aggregates
    )
    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    dr = driver.Driver({"path": path, "qc": qc}, temp_module, adapter=adapter)
    # dr.visualize_execution(['computed_aggregates'], './my_dag.dot', {})
    result = dr.execute(["computed_aggregates"])
    return result["computed_aggregates"].collect()


def hello_world_main(qc, path: str):
    lineitem = qc.read_csv(path + "lineitem.tbl", sep="|", has_header=True)
    d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
    print(len(d.schema))
    d = d.with_column(
        "disc_price",
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]),
        required_columns={"l_extendedprice", "l_discount"},
    )
    print(len(d.schema))
    d = d.with_column(
        "charge",
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) * (1 + x["l_tax"]),
        required_columns={"l_extendedprice", "l_discount", "l_tax"},
    )
    print(len(d.schema))
    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag", "l_linestatus"]).agg(
        {
            "l_quantity": ["sum", "avg"],
            "l_extendedprice": ["sum", "avg"],
            "disc_price": "sum",
            "charge": "sum",
            "l_discount": "avg",
            "*": "count",
        }
    )
    print(f.schema)
    print(len(f.schema))
    df = f.collect()
    print(df.columns)
    print(len(df.columns))
    return df


"""
lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True)
customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True)
part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True)
supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True)
partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True)
nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True)
region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True)
"""


def do_12(qc, path: str):
    lineitem = qc.read_csv(path + "lineitem.tbl", sep="|", has_header=True)
    orders = qc.read_csv(path + "orders.tbl", sep="|", has_header=True)
    d = lineitem.join(orders, left_on="l_orderkey", right_on="o_orderkey")
    d = d.filter(
        "l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'"
    )
    d = d.with_column(
        "high",
        lambda x: (x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"),
        required_columns={"o_orderpriority"},
    )
    d = d.with_column(
        "low",
        lambda x: (x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH"),
        required_columns={"o_orderpriority"},
    )
    f = d.groupby("l_shipmode").aggregate(aggregations={"high": ["sum"], "low": ["sum"]})
    return f.collect()


def do_3(qc, path: str):
    lineitem: DataStream = qc.read_csv(path + "lineitem.tbl", sep="|", has_header=True)
    lineitem = lineitem.select(
        [
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
    orders = qc.read_csv(path + "orders.tbl", sep="|", has_header=True)
    orders = orders.select(["o_orderkey", "o_orderdate", "o_shippriority", "o_custkey"])
    customer = qc.read_csv(path + "customer.tbl", sep="|", has_header=True)
    customer = customer.select(["c_custkey", "c_mktsegment"])
    d = lineitem.join(orders, left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d, left_on="c_custkey", right_on="o_custkey")
    d = d.select(
        [
            "l_orderkey",
            "c_mktsegment",
            "o_orderdate",
            "l_shipdate",
            "l_extendedprice",
            "l_discount",
            "o_shippriority",
        ]
    )
    # d = d.filter("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_column(
        "revenue",
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]),
        required_columns={"l_extendedprice", "l_discount"},
    )
    d = d.select(["revenue", "o_orderdate", "o_shippriority", "l_orderkey"])
    f = d.groupby(["l_orderkey", "o_orderdate", "o_shippriority"]).agg({"revenue": ["sum"]})
    return f.collect()


if __name__ == "__main__":
    qc_ = QuokkaContext()
    path_ = "/Users/stefankrawczyk/Downloads/tpc-h-public/"
    # df = hello_world_main(qc_, path_)
    # print(df)
    # df = hello_world_hamilton(qc_, path_)
    # print(df)
    # df = manual_hello_world_hamilton(qc_, path_)
    df = do_3(qc_, path_)
    print(df)
    print(len(df))
    print(len(df.columns))
    print(df.columns)
