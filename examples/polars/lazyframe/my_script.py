import logging
import sys

from hamilton import base, driver
from hamilton.plugins import h_polars_lazyframe

logging.basicConfig(stream=sys.stdout)

# Create a driver instance. If you want to run the compute in the final node you can also use
# h_polars.PolarsDataFrameResult() and you don't need to run collect at the end. Which you use
# probably depends on whether you want to use the LazyFrame in more nodes in another DAG before
# computing the result.
adapter = base.SimplePythonGraphAdapter(result_builder=h_polars_lazyframe.PolarsLazyFrameResult())
import my_functions  # where our functions are defined

dr = driver.Driver({}, my_functions, adapter=adapter)
output_columns = [
    "spend_per_signup",
]
# let's create the lazyframe!
df = dr.execute(output_columns)
# Here we just print the Lazyframe plan
print(df)

# Now we run the query
df = df.collect()

# And print the table.
print(df)

# To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
# dr.visualize_execution(output_columns, './polars', {"format": "png"})
# dr.display_all_functions('./my_full_dag.dot')
