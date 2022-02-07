import importlib
import logging
import sys

import pandas as pd
from hamilton import driver

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout)
initial_columns = {}  # empty because no conditional logic in creating the DAG.
# we need to tell hamilton where to load function definitions from
module_name = 'my_functions'
module = importlib.import_module(module_name)
dr = driver.Driver(initial_columns, module)  # can pass in multiple modules
# we need to specify what we want in the final dataframe.
output_columns = [
    'spend',
    'signups',
    'avg_3wk_spend',
    'spend_per_signup',
    'spend_zero_mean_unit_variance'
]
# let's create the dataframe!
# Load the chunked data.
chunks = [
    {'spend': pd.Series([10, 10, 20]), 'signups': pd.Series([1, 10, 50])},
    {'spend': pd.Series([40, 40, 50]), 'signups': pd.Series([100, 200, 400])}
]
dfs = []
for idx, chunk in enumerate(chunks):
    df_chunk = dr.execute(output_columns, inputs=chunk)
    # To visualize do `pip install sf-hamilton[visualization]` if you want these to work
    # dr.visualize_execution(output_columns, f'./my_dag_{idx}.dot', {}, inputs=chunk)
    print(df_chunk.to_string())
    dfs.append(df_chunk)

# joining them together.
df = pd.concat(dfs)
print(df.to_string())

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
# dr.display_all_functions('./my_full_dag.dot')
