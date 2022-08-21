# Cell 1 - import the things you need
import logging
import sys

import numpy as np
import pandas as pd

from hamilton import ad_hoc_utils, driver

logging.basicConfig(stream=sys.stdout)

# Cell 2 - import modules to create part of the DAG from
import my_functions


# Cell 3 - Define your new Hamilton functions & curate them into a TemporaryFunctionModule object.
# Look at `my_functions` to see how these functions connect.
def signups() -> pd.Series:
    """Returns sign up values"""
    return pd.Series([1, 10, 50, 100, 200, 400])


def spend() -> pd.Series:
    """Returns the spend values"""
    return pd.Series([10, 10, 20, 40, 40, 50])


def log_spend_per_signup(spend_per_signup: pd.Series) -> pd.Series:
    """Simple function taking the logarithm of spend over signups."""
    return np.log(spend_per_signup)


# Place the functions into a temporary module -- the idea is that this should house a curated set of functions.
# Don't be afraid to make multiple of them -- however we'd advise you to not use this method for production.
# Also note, that using a temporary function module does not work for scaling onto Ray, Dask, or Pandas on Spark.
temp_module = ad_hoc_utils.create_temporary_module(
    spend, signups, log_spend_per_signup, module_name="function_example"
)

# Cell 4 - Instantiate the Hamilton driver and pass it the right things in.
initial_config = {}
# we need to tell hamilton where to load function definitions from
dr = driver.Driver(initial_config, my_functions, temp_module)  # can pass in multiple modules
# we need to specify what we want in the final dataframe.
output_columns = [
    "spend",
    "signups",
    "avg_3wk_spend",
    "spend_per_signup",
    "spend_zero_mean_unit_variance",
    "log_spend_per_signup",
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df.to_string())

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
# dr.visualize_execution(output_columns, './my_dag.dot', {})
# dr.display_all_functions('./my_full_dag.dot')
