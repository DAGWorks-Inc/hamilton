from hamilton import driver
import data_loader
import common

config = {"schema_version": "old"}
dr = driver.Driver(config, data_loader, common)
result = dr.execute(
    [common.orders_by_order_aggregates],
    inputs={
        "orders_path": "../data/orders_old.csv",
        "order_details_path": "../data/order_details.csv",
    },
)
print(result)
