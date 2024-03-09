from typing import Dict

import numpy as np
import pandas
import pandas as pd
from pandas import DataFrame, Series

from hamilton.function_modifiers import pipe, step, extract_fields, extract_columns, inject, source

# ----------------------------------------------------------------------------------------------------------------------
# Tax Rate & Credit rules
# ----------------------------------------------------------------------------------------------------------------------

tax_rates_rules = {
    "Income < 50000": 0.15,  # < 50k: Tax rate is 15 %
    "Income > 50000 and Income < 70000": 0.18,  # 50k to 70k: Tax rate is 18 %
    "Income > 70000 and Income < 100000": 0.2,  # 70k to 100k: Tax rate is 20 %
    "Income > 100000 and Income < 120000": 0.22,  # 100k to 120k: Tax rate is 22 %
    "Income > 120000 and Income < 150000": 0.25,  # 120k to 150k: Tax rate is 25 %
    "Income > 150000": 0.28,  # over 150k: Tax rate is 28 %
}

tax_credits_rules = {
    "Children == 0": 0,  # 0 child: Tax credit 0 %
    "Children == 1": 0.02,  # 1 child: Tax credit 2 %
    "Children == 2": 0.04,  # 2 children: Tax credit 4 %
    "Children == 3": 0.06,  # 3 children: Tax credit 6 %
    "Children == 4": 0.08,  # 4 children: Tax credit 8 %
    "Children > 4": 0.1,  # over 4 children: Tax credit 10 %
}


# ----------------------------------------------------------------------------------------------------------------------
# Tax calculation functions
# ----------------------------------------------------------------------------------------------------------------------

def tax_rate(df: DataFrame, tax_rates: Dict[str, float]) -> DataFrame:
    """
    Add a series 'Tax Rate' to the DataFrame based on the tax_rates rules.
    :param df: The DataFrame
    :param tax_rates: Tax rates rules
    :return: the DataFrame with the 'Tax Rate' Series
    """
    output = DataFrame()
    for tax_rate_formula, tax_rate in tax_rates.items():
        selected = df.query(tax_rate_formula)
        if selected.empty:
            continue
        tmp = DataFrame({"Tax Rate": tax_rate}, index=selected.index)
        output = pd.concat([output, tmp], axis=0)
    df["Tax Rate"] = output["Tax Rate"]
    return df


def tax_credit(df: DataFrame, tax_credits: Dict[str, float]) -> DataFrame:
    """
    Add a series 'Tax Credit' to the DataFrame based on the tax_credits rules.
    :param df: The DataFrame
    :param tax_credits: Tax credits rules
    :return: the DataFrame with the 'Tax Credit' Series
    """
    output = DataFrame()
    for tax_credit_formula, tax_credit in tax_credits.items():
        selected = df.query(tax_credit_formula)
        if selected.empty:
            continue
        tmp = DataFrame({"Tax Credit": tax_credit}, index=selected.index)
        output = pd.concat([output, tmp], axis=0)
    df["Tax Credit"] = output["Tax Credit"]
    return df


@inject(income=source("Income"), tax_rate=source("Tax Rate"), tax_credit=source("Tax Credit"))
def tax_formula(income: Series, tax_rate: Series, tax_credit: Series) -> Series:
    """
    Return a DataFrame with a series 'Tax Formula' from 'Income', 'Tax Rate' and 'Tax Credit' series.

    :param income: the 'Income' series
    :param tax_rate: the 'Tax Rate' series
    :param tax_credit: the 'Tax Credit' series

    :return: the DataFrame with the 'Tax Formula' Series
    """
    df = DataFrame({'income': income, 'tax_rate': tax_rate, 'tax_credit': tax_credit})
    df["Tax Formula"] = df.apply(
        lambda x: (
            f"({int(x['income'])} * {x['tax_rate']})"
            if np.isnan(x["tax_credit"]) else
            f"({int(x['income'])} * {x['tax_rate']}) - ({int(x['income'])} * {x['tax_rate']}) * {x['tax_credit']}"
        ), axis=1
    )
    return df["Tax Formula"]


def tax(tax_formula: Series) -> Series:
    """
    Return a series 'Tax' from 'Tax Formula' series.
    :param tax_formula: the 'Tax Formula' series.
    :return: the 'Tax Formula' Series
    """
    df = tax_formula.to_frame()
    df["Tax"] = df["Tax Formula"].apply(lambda x: round(pandas.eval(x)))
    return df["Tax"]


# ----------------------------------------------------------------------------------------------------------------------
# Tax calculation pipelines (chained set of transformations)
# ----------------------------------------------------------------------------------------------------------------------

@pipe(
    step(tax_rate, tax_rates=tax_rates_rules),  # apply the tax_rate step
    step(tax_credit, tax_credits=tax_credits_rules),  # apply the tax_credit step
)
def under_100k_tax(under_100k: DataFrame) -> DataFrame:
    """
    Tax calculation pipeline for 'Income' under 100k.
    :param df: The DataFrame  where 'Income' is under 100k
    :return: the DataFrame with the 'Tax' Series
    """
    return under_100k


@pipe(
    step(tax_rate, tax_rates=tax_rates_rules),  # apply the tax_rate step
)
def over_100k_tax(over_100k: DataFrame) -> DataFrame:
    """
    Tax calculation pipeline for 'Income' over 100k.
    :param over_100k: The DataFrame where 'Income' is over 100k
    :return: the DataFrame with the 'Tax' Series
    """
    return over_100k


# ----------------------------------------------------------------------------------------------------------------------
# Tax calculator dataflow
# ----------------------------------------------------------------------------------------------------------------------

@extract_fields(
    {"under_100k": DataFrame, "over_100k": DataFrame}
)
# This is the first node in the DAG
def split_dataframe(input: DataFrame) -> Dict[str, DataFrame]:
    """
    That function takes the DataFrame in input and split it in 2 DataFrames:
      - under_100k: Rows where 'Income' is under 100k
      - over_100k: Rows where 'Income' is over 100k
    """
    return {
        "under_100k": input.query('Income < 100000'),
        "over_100k": input.query('Income >= 100000'),
    }


@extract_columns('Income', 'Tax Rate', 'Tax Credit')
def combine_dataframe(under_100k_tax: DataFrame, over_100k_tax: DataFrame) -> DataFrame:
    """
    That function combine the DataFrames under_100k and over_100k

    The @extract_columns decorator is making the Series available for processing.
    """
    combined = pd.concat([under_100k_tax, over_100k_tax], axis=0).sort_index()
    return combined


# This is the last node in the DAG
def end(combine_dataframe: DataFrame, tax_formula: Series, tax: Series) -> DataFrame:
    """
    That function combine the DataFrames
    """
    df = combine_dataframe.copy(deep=True)

    df["Tax Formula"] = tax_formula
    df["Tax"] = tax
    df["Tax Rate"] = df["Tax Rate"].apply(lambda x: f"{int(x * 100)} %")
    df["Tax Credit"] = df["Tax Credit"].apply(lambda x: f"{int(x * 100)} %" if not np.isnan(x) else "")

    order = ["Name", "Income", "Children", "Tax Rate", "Tax Credit", "Tax", "Tax Formula"]
    return df.reindex(columns=order)
