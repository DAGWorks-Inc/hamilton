from typing import Dict

from pandas import DataFrame

from hamilton import driver
import my_functions

driver = driver.Driver({}, my_functions)


class TaxCalculator:
    """
    Simple class to wrap Hamilton Driver
    """

    @staticmethod
    def calculate(input: DataFrame, tax_rates: Dict[str, float], tax_credits: Dict[str, float]) -> DataFrame:
        return driver.execute(inputs={
            "input": input,
            "tax_rates": tax_rates,
            "tax_credits": tax_credits
        }, final_vars=["final_tax_dataframe"])

    @staticmethod
    def visualize(output_path="./my_full_dag.png"):
        # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
        driver.display_all_functions(output_path)
