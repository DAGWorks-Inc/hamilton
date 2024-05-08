# run.py
import pandas as pd
from hamilton import driver
import dataflow

dr = driver.Builder().with_modules(dataflow).build()
# ^ from Step 2
inputs = dict(
    companies=pd.read_parquet("path/to/companies.parquet"),
    shuttles=pd.read_parquet("path/to/shuttles.parquet"),
)
results = dr.execute(["model_input_table"], inputs=inputs)
# results is a dict {"model_input_table": VALUE}