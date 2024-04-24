import pandas as pd
import pandera as pa

from hamilton.function_modifiers import check_output, config, source
from hamilton.function_modifiers.adapters import load_from

PRODUCT_SET = frozenset(["Apple", "Orange", "Banana", "Grape", "Pineapple", "Watermelon"])
order_details_schema = pa.DataFrameSchema(
    {
        "order_id": pa.Column(int, checks=[pa.Check.ge(0)], nullable=False),
        "product_name": pa.Column(
            str,
            checks=[pa.Check.isin(PRODUCT_SET)],
            nullable=False,
        ),
        "unit_price": pa.Column(
            float,
            checks=[pa.Check.ge(0.0), pa.Check.less_than_or_equal_to(10000.0)],
            nullable=False,
        ),
    },
    strict=True,
)


@check_output(schema=order_details_schema, importance="fail")
@load_from.csv(path=source("order_details_path"), sep=",")
def order_details(df: pd.DataFrame) -> pd.DataFrame:
    return df


orders_schema = pa.DataFrameSchema(
    {
        "order_id": pa.Column(int, checks=[pa.Check.ge(0)], nullable=False),
        "customer_name": pa.Column(str, nullable=False),
        "order_date": pa.Column(pa.dtypes.Timestamp, nullable=False),
        "product_name": pa.Column(
            str,
            checks=[pa.Check.isin(PRODUCT_SET)],
            nullable=False,
        ),
        "quantity": pa.Column(
            int, checks=[pa.Check.ge(0), pa.Check.less_than_or_equal_to(1000)], nullable=False
        ),
    },
    strict=True,
)


@config.when(schema_version="old")
@check_output(schema=orders_schema, importance="fail")
@load_from.csv(path=source("orders_path"), sep=",")
def orders__old(df: pd.DataFrame) -> pd.DataFrame:
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_list"] = df.apply(
        lambda x: [
            f"{x.product1}-{x.quantity1}",
            f"{x.product2}-{x.quantity2}",
            f"{x.product3}-{x.quantity3}",
        ],
        axis=1,
    )
    for x in ["product1", "product2", "product3", "quantity1", "quantity2", "quantity3"]:
        del df[x]
    df = df.explode("order_list")
    df["product_name"] = df["order_list"].apply(lambda x: x.split("-")[0])
    df["quantity"] = df["order_list"].apply(lambda x: int(x.split("-")[1]))
    del df["order_list"]
    return df


@config.when(schema_version="new")
@check_output(schema=orders_schema, importance="fail")
@load_from.csv(path=source("orders_path"), sep=",")
def orders__new(df: pd.DataFrame) -> pd.DataFrame:
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df
