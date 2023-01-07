"""Shows how you can use Hamilton with Quokka today.

There isn't any transform re-use due to relying on mutation order
of the central DataStream object.
"""
from pyquokka.datastream import DataStream, GroupedDataStream
from pyquokka.df import QuokkaContext


def lineitem(qc: QuokkaContext, path: str) -> DataStream:
    """Loads and filters data from the lineitem table"""
    ds = qc.read_csv(path, sep="|", has_header=True)
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
    lineitem = qc.read_csv(path, sep="|", has_header=True)
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


if __name__ == "__main__":
    qc_ = QuokkaContext()
    path_ = "/Users/stefankrawczyk/Downloads/tpc-h-public/lineitem.tbl"
    df = hello_world_main(qc_, path_)
    # print(df)
    # df = hello_world_hamilton(qc_, path_)
    # print(df)
    # df = manual_hello_world_hamilton(qc_, path_)
    print(df)
    print(len(df.columns))
    print(df.columns)
