import logging
import pathlib
import sys

import business_logic
import data_loaders

from hamilton import base, driver
from hamilton.experimental import h_cache

logging.basicConfig(stream=sys.stdout)

# This is empty, we get the data from the data_loaders module
initial_columns = {}

cache_path = "tmp"
pathlib.Path(cache_path).mkdir(exist_ok=True)
adapter = h_cache.CachingGraphAdapter(cache_path, base.PandasDataFrameResult())
dr = driver.Driver(initial_columns, business_logic, data_loaders, adapter=adapter)
output_columns = [
    data_loaders.spend,
    data_loaders.signups,
    business_logic.avg_3wk_spend,
    business_logic.spend_per_signup,
    business_logic.spend_zero_mean_unit_variance,
]

df = dr.execute(output_columns)
print(df.to_string())
