import logging
import sys

from hamilton import base, driver
from hamilton.plugins import polars_implementations

logging.basicConfig(stream=sys.stdout)

# Create a driver instance.
adapter = base.SimplePythonGraphAdapter(
    result_builder=polars_implementations.PolarsDataFrameResult()
)
config = {
    "base_df_location": "dummy_value",
}
import my_functions  # where our functions are defined

dr = driver.Driver(config, my_functions, adapter=adapter)
# note -- we cannot request scalar outputs like we could do with Pandas.
output_columns = [
    "spend",
    "signups",
    "avg_3wk_spend",
    "spend_per_signup",
    "spend_zero_mean_unit_variance",
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df)

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
# dr.visualize_execution(output_columns, './my_dag.dot', {})
# dr.display_all_functions('./my_full_dag.dot')
