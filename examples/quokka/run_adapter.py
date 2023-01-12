import possible_api
import quokka_adapter
from pyquokka.df import QuokkaContext

from hamilton import driver


def tpc_1(qc, path):
    adapter = quokka_adapter.QuokkaGraphAdapter()
    dr = driver.Driver(
        {
            "qc": qc,
            "path": path,
            "lineitem_filter": "l_shipdate <= date '1998-12-01' - interval '90' day",
        },
        possible_api,
        adapter=adapter,
    )
    outputs = [
        "al_returnflag",  # can comment one of these out and they are dropped
        "al_linestatus",
        "__count_sum",
        "charge_sum",
        "disc_price_sum",
        "l_discount_mean",
        "l_extendedprice_mean",
        "l_extendedprice_sum",
        "l_quantity_mean",
        "l_quantity_sum",
        # "grouped_lineitem" -- calling collect on groups doesn't work it seems...
        # "l_returnflag",  # can also get intermediate results
        # "charge",
        # "disc_price",
    ]
    dr.visualize_execution(outputs, "./tpc_1.dot", {})
    result = dr.execute(outputs)
    print(result.columns)
    print(result)


def tpc_3(qc, path):
    adapter = quokka_adapter.QuokkaGraphAdapter()
    dr = driver.Driver({"qc": qc, "path": path}, possible_api, adapter=adapter)
    outputs = ["aclo_orderkey", "aclo_orderdate", "aclo_shippriority", "aclo_revenue_sum"]
    dr.visualize_execution(outputs, "./tpc_3.dot", {})
    result = dr.execute(outputs)
    print(result.columns)
    print(result)


if __name__ == "__main__":
    qc = QuokkaContext()
    path = "/Users/stefankrawczyk/Downloads/tpc-h-public/"
    # tpc_1(qc, path)
    tpc_3(qc, path)
