import logging
import sys

from hamilton import base, driver
from hamilton.plugins import h_vaex

logging.basicConfig(stream=sys.stdout)

# Create a driver instance.
adapter = base.SimplePythonGraphAdapter(result_builder=h_vaex.VaexDataFrameResult())
config = {
    "base_df_location": "dummy_value",
}

# where our functions are defined
import my_functions

dr = driver.Driver(config, my_functions, adapter=adapter)
output_columns = [
    "spend",
    "signups",
    "spend_per_signup",
    "spend_std_dev",
    "spend_mean",
    "spend_zero_mean_unit_variance",
]

# let's create the dataframe!
df = dr.execute(output_columns)
print(df)
