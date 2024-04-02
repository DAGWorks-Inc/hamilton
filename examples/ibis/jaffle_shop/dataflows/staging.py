import ibis
import ibis.expr.types as ir


def customers(connection: ibis.BaseBackend, customers_source: str) -> ir.Table:
    """raw data about customers

    columns:
        - customer_id
        - first_name
        - last_name
    """
    return connection.read_parquet(customers_source, table_name="customers")[
        "id", "first_name", "last_name"
    ].rename(customer_id="id")


def orders(connection: ibis.BaseBackend, orders_source: str) -> ir.Table:
    """raw data about orders

    columns:
        - order_id
        - customer_id
        - order_date
        - status
    """
    return connection.read_parquet(orders_source, table_name="orders")[
        "id", "user_id", "order_date", "status"
    ].rename(order_id="id", customer_id="user_id")


def payments(connection: ibis.BaseBackend, payments_source: str) -> ir.Table:
    """raw data about payments; convert amount from cents to dollars

    columns:
        - payment_id
        - order_id
        - payment_method
        - amount
    """
    return (
        connection.read_parquet(payments_source, table_name="payments")[
            "id", "order_id", "payment_method", "amount"
        ]
        .rename(payment_id="id")
        .mutate(amount=ibis._.amount / 100)
    )
