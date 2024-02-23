import pandas as pd

from hamilton.function_modifiers import extract_columns


def customers_df(customers_path: str = "customers.csv") -> pd.DataFrame:
    """Load the customer dataset."""
    return pd.read_csv(customers_path)


def orders_df(orders_path: str = "orders.csv") -> pd.DataFrame:
    """Load the orders dataset."""
    return pd.read_csv(orders_path)


@extract_columns("amount", "age", "country")
def customers_orders_df(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Combine the customers and orders datasets.
    Setting index to (order_id, customer_id)."""
    _df = pd.merge(customers_df, orders_df, on="customer_id")
    _df = _df.set_index(["order_id", "customer_id"])
    return _df


def orders_per_distributor(customers_orders_df: pd.DataFrame) -> pd.Series:
    """Compute the number of orders per customer.
    Outputs series indexed by customer_id."""
    return customers_orders_df.groupby("customer_id").size().rename("orders_per_distributor")


def average_order_by_customer(amount: pd.Series) -> pd.Series:
    """Compute the average order amount per customer.
    Outputs series indexed by customer_id."""
    return (amount.groupby("customer_id").mean().rename("average_order_by_customer")) + 1


def customer_summary_table(
    orders_per_distributor: pd.Series, average_order_by_customer: pd.Series
) -> pd.DataFrame:
    """Our customer summary table definition."""
    return pd.concat([orders_per_distributor, average_order_by_customer], axis=1)
