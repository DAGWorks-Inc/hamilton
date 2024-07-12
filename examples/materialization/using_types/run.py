import simple_etl
from hamilton_sdk import adapters

from hamilton import driver

tracker = adapters.HamiltonTracker(
    project_id=7,  # modify this as needed
    username="elijah@dagworks.io",
    dag_name="my_version_of_the_dag",
    tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
)  # note this slows down execution because there's 60 columns.
# 30 columns adds about a 1 second.
# 60 is therefore 2 seconds.

dr = driver.Builder().with_config({}).with_modules(simple_etl).with_adapters(tracker).build()
dr.display_all_functions("simple_etl.png")

import time

start = time.time()
print(start)
dr.execute(["saved_data"], inputs={"filepath": "data.csv"})
print(time.time() - start)
