import polars as pl

from hamilton.function_modifiers import config
from hamilton.plugins.h_polars_lazyframe import with_columns


def upstream_factor() -> int:
    return 3


def initial_df() -> pl.LazyFrame:
    return pl.DataFrame(
        {"col_1": [1, 2, 3, 4], "col_2": [11, 12, 13, 14], "col_3": [1, 1, 1, 1]}
    ).lazy()


def subtract_1_from_2(col_1: pl.Expr, col_2: pl.Expr) -> pl.Expr:
    return col_2 - col_1


@config.when(factor=5)
def multiply_3__by_5(col_3: pl.Expr) -> pl.Expr:
    return col_3 * 5


@config.when(factor=7)
def multiply_3__by_7(col_3: pl.Expr) -> pl.Expr:
    return col_3 * 7


def add_1_by_user_adjustment_factor(col_1: pl.Expr, user_factor: int) -> pl.Expr:
    return col_1 + user_factor


def multiply_2_by_upstream_3(col_2: pl.Expr, upstream_factor: int) -> pl.Expr:
    return col_2 * upstream_factor


@with_columns(
    subtract_1_from_2,
    multiply_3__by_5,
    multiply_3__by_7,
    add_1_by_user_adjustment_factor,
    multiply_2_by_upstream_3,
    columns_to_pass=["col_1", "col_2", "col_3"],
    select=[
        "subtract_1_from_2",
        "multiply_3",
        "add_1_by_user_adjustment_factor",
        "multiply_2_by_upstream_3",
    ],
    namespace="some_subdag",
)
def final_df(initial_df: pl.LazyFrame) -> pl.LazyFrame:
    return initial_df


def col_1(initial_df: pl.LazyFrame) -> pl.Expr:
    return pl.col("col_1")


@config.when(factor=5)
def multiply_1__by_5(col_1: pl.Expr) -> pl.Expr:
    return col_1 * 5


@config.when_not(factor=5)
def multiply_1__by_1(col_1: pl.Expr) -> pl.Expr:
    return col_1 * 1


@with_columns(
    col_1,
    multiply_1__by_5,
    multiply_1__by_1,
    on_input="initial_df",
    select=["col_1", "multiply_1"],
)
def final_df_2(initial_df: pl.LazyFrame) -> pl.LazyFrame:
    return initial_df
