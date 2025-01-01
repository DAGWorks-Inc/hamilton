import time

import my_functions

from hamilton import driver
from hamilton.plugins import h_threadpool

start = time.time()
adapter = h_threadpool.FutureAdapter()
dr = driver.Builder().with_modules(my_functions).with_adapters(adapter).build()
dr.display_all_functions("my_funtions.png")
r = dr.execute("s")
print("got return from dr")
print(r)
print("Time taken with", time.time() - start)

from hamilton_sdk import adapters

tracker = adapters.HamiltonTracker(
    project_id=21,  # modify this as needed
    username="elijah@dagworks.io",
    dag_name="with_caching",
    tags={"environment": "DEV", "cached": "False", "team": "MY_TEAM", "version": "1"},
)

start = time.time()
dr = (
    driver.Builder().with_modules(my_functions).with_adapters(tracker, adapter).with_cache().build()
)
dr.display_all_functions("a.png")
r = dr.execute("s")
print("got return from dr")
print(r)
print("Time taken with cold cache", time.time() - start)

tracker = adapters.HamiltonTracker(
    project_id=21,  # modify this as needed
    username="elijah@dagworks.io",
    dag_name="with_caching",
    tags={"environment": "DEV", "cached": "True", "team": "MY_TEAM", "version": "1"},
)

start = time.time()
dr = (
    driver.Builder().with_modules(my_functions).with_adapters(tracker, adapter).with_cache().build()
)
dr.display_all_functions("a.png")
r = dr.execute("s")
print("got return from dr")
print(r)
print("Time taken with warm cache", time.time() - start)

start = time.time()
dr = driver.Builder().with_modules(my_functions).build()
dr.display_all_functions("a.png")
r = dr.execute("s")
print("got return from dr")
print(r)
print("Time taken without", time.time() - start)
