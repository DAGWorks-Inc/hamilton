import time

import my_functions

from hamilton import async_driver, driver
from hamilton.plugins import h_threadpool

start = time.time()
adapter = h_threadpool.FutureAdapter()
dr = driver.Builder().with_modules(my_functions).with_adapters(adapter).build()
dr.display_all_functions("my_funtions.png")
r = dr.execute(["s", "x", "a"])
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
r = dr.execute(["s", "x", "a"])
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
r = dr.execute(["s", "x", "a"])
print("got return from dr")
print(r)
print("Time taken with warm cache", time.time() - start)

start = time.time()
dr = driver.Builder().with_modules(my_functions).build()
r = dr.execute(["s", "x", "a"])
print("got return from dr")
print(r)
print("Time taken without", time.time() - start)


async def run_async():
    import my_functions_async

    start = time.time()
    dr = await async_driver.Builder().with_modules(my_functions_async).build()
    dr.display_all_functions("my_functions_async.png")
    r = await dr.execute(["s", "x", "a"])
    print("got return from dr")
    print(r)
    print("Async Time taken without", time.time() - start)


import asyncio

asyncio.run(run_async())
