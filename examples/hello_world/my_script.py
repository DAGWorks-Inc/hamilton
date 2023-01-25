# import importlib
import logging
import sys

import pandas as pd

from hamilton import driver

logging.basicConfig(stream=sys.stdout)
initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
    # Note: these values don't have to be all series, they could be a scalar.
    "signups": pd.Series([1, 10, 50, 100, 200, 400]),
    "spend": pd.Series([10, 10, 20, 40, 40, 50]),
}
# we need to tell hamilton where to load function definitions from
# programmatic code to load modules:
# module_name = "my_functions"
# my_functions = importlib.import_module(module_name)
# or import module(s) directly:
import my_functions

dr = driver.Driver(initial_columns, my_functions)  # can pass in multiple modules
# we need to specify what we want in the final dataframe. These can be string names, or function references.
output_columns = [
    "spend",
    "signups",
    my_functions.avg_3wk_spend,  # could just pass "avg_3wk_spend" here
    my_functions.spend_per_signup,  # could just pass "spend_per_signup" here
    my_functions.spend_zero_mean_unit_variance,  # could just pass "spend_zero_mean_unit_variance" here
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df.to_string())

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
dr.visualize_execution(output_columns, "./my_dag.dot", {"format": "png"})
dr.display_all_functions("./my_full_dag.dot", {"format": "png"})
