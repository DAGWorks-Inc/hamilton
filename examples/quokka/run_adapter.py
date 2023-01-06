import possible_api
import quokka_adapter
from pyquokka.df import QuokkaContext

from hamilton import driver

if __name__ == "__main__":
    qc = QuokkaContext()
    path = "/Users/stefankrawczyk/Downloads/tpc-h-public/lineitem.tbl"
    adapter = quokka_adapter.QuokkaGraphAdapter()
    dr = driver.Driver({"qc": qc, "path": path}, possible_api, adapter=adapter)
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
    dr.visualize_execution(outputs, "./my_dag.dot", {})
    result = dr.execute(outputs)
    print(result.columns)
    print(result)
