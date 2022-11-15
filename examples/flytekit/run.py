import importlib
import logging
import sys

import pandas as pd

from hamilton import base, driver
from hamilton.experimental import h_flytekit

logging.basicConfig(stream=sys.stdout)
initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
    # Note: these values don't have to be all series, they could be a scalar.
    "signups": pd.Series([1, 10, 50, 100, 200, 400]),
    "spend": pd.Series([10, 10, 20, 40, 40, 50]),
}
# we need to tell hamilton where to load function definitions from
module_name = "my_functions"
module = importlib.import_module(module_name)
fga = h_flytekit.FlyteKitAdapter()
dr = driver.Driver(initial_columns, module, adapter=fga)  # can pass in multiple modules
# we need to specify what we want in the final dataframe.
output_columns = [
    "spend",
    "signups",
    "avg_3wk_spend",
    "spend_per_signup",
    "spend_zero_mean_unit_variance",
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df.to_string())

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
# dr.visualize_execution(output_columns, './my_dag.dot', {})
# dr.display_all_functions('./my_full_dag.dot')
