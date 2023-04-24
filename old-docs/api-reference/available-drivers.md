---
description: API docs for using the drivers
---

# Available Drivers

Currently, we have a single driver. It's highly parametrizable, allowing you to customize:

* The way the DAG is executed (how each node is executed), i.e. either locally, in parallel, or on a cluster!
* How the results are materialized back to you -- e.g. a DataFrame, a dictionary, your custom object!

To tune the above, pass in a Graph Adapter and or Result Builder-- see [available-result-builders.md](available-result-builders.md "mention") & [available-graph-adapters.md](available-graph-adapters.md "mention").

## Hamilton Driver Usage

Let's walk through how you might use the Hamilton Driver.

### Instantiation

1. Determine the configuration required to setup the DAG.
2. Provide the python modules that should be crawled to create the DAG.
3. Optional. Determine the return type of the object you want `execute()` to return. Default is to create a Pandas DataFrame.ho

```python
from hamilton import driver
from hamilton import base

# 1. Setup config. See the Parameterizing the DAG section for usage
config = {} 

# 2. we need to tell hamilton where to load function definitions from
module_name = 'my_functions'
module = importlib.import_module(module_name)  # or simply "import my_functions"

# 3. Determine the return type -- default is a pandas.DataFrame.
adapter = base.SimplePythonDataFrameGraphAdapter() # See GraphAdapter docs for more details.

# These all feed into creating the driver & thus DAG.
dr = driver.Driver(config, module, adapter=adapter)
```

### Execution

#### Using a DAG once

This approach assumes that all inputs were passed in with the `config` dictionary above.

```python
output = ['output1', 'output2', ...]
df = dr.execute(output)
```

#### Using a DAG multiple times

This approach assumes that at least one input is not provided in the `config` dictionary provided to the constructor, and instead you provide that input to each `execute` invocation.

```python
output = ['output1', 'output2', ...]
for data in dataset:  # if data is a dict of values.
    df = dr.execute(output, inputs=data)
```

#### Short circuiting some DAG computation

This will force Hamilton to short circuit a particular computation path, and use the passed in override as a result of that particular node.

```python
output = ['output1', 'output2', ...]
df = dr.execute(output, overrides={'intermediate_node': intermediat_value})
```

##
