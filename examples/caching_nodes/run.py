import logging
import sys

from hamilton import driver

import data_loaders
import business_logic

logging.basicConfig(stream=sys.stdout)

# This is empty, we get the data from the data_loaders module
initial_columns = {}

dr = driver.Driver(initial_columns, business_logic, data_loaders)
output_columns = [
    data_loaders.spend,
    data_loaders.signups,
    business_logic.avg_3wk_spend,
    business_logic.spend_per_signup,
    business_logic.spend_zero_mean_unit_variance,
]

df = dr.execute(output_columns)
print(df.to_string())
