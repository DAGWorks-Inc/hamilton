import pandas as pd


def orders_denormalized(orders: pd.DataFrame, order_details: pd.DataFrame) -> pd.DataFrame:
    _df = pd.merge(orders, order_details, how="inner", on=["order_id", "product_name"])
    _df["item_total"] = _df["unit_price"] * _df["quantity"]
    return _df


def orders_by_order_aggregates(orders_denormalized: pd.DataFrame) -> pd.DataFrame:
    _df = orders_denormalized.groupby(["order_id", "customer_name", "order_date"]).agg(
        {"item_total": "sum", "quantity": "sum"}
    )
    return _df
