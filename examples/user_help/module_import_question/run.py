import hamilton_plain_functions

from hamilton import base, driver

config = {"ma_type": "v1"}
adapter = base.SimplePythonGraphAdapter(base.DictResult())
dr = driver.Driver(config, hamilton_plain_functions, adapter=adapter)
result = dr.execute(
    ["moving_average", "some_other_function"], inputs={"input": {}, "other_input": 2.0}
)
print(result)
