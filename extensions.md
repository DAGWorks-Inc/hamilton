---
description: >-
  Hamilton structurally provides the foundation for several extensions: parallel
  computation, distributed computation, creating optimized machine code,
  controlling return object types.
---

# Extensions

## Scaling Hamilton: Parallel & Distributed Computation

Hamilton by default runs in a single process and single threaded manner.

Wouldn't it be great if it could execute computation in parallel if it could? Or, if you could scale to data sets that can't all fit in memory?

Well, with the simple change of some driver script code, you can very easily scale your Hamilton dataflows!

```
from hamilton import driver
from hamilton.experimental import h_dask  # import the correct module

from dask.distributed import Client  # import the distributed system of choice
client = Client(...)  # instantiate the specific client

dag_config = {...} 
bl_module = importlib.import_module('my_functions') # business logic functions 
loader_module = importlib.import_module('data_loader') # functions to load data
adapter = h_dask.DaskGraphAdapter(client) # create the right GraphAdapter

dr = driver.Driver(dag_config, bl_module, loader_module, adapter=adapter) 
output_columns = ['year','week',...,'spend_shift_3weeks_per_signup','special_feature'] 
df = dr.execute(output_columns) # only walk DAG for what is needed
```

### A note on the definition of _Experimental_

TL;DR: the code is stable, but it needs more bake time & feedback!

The following implementations are considered experimental because they require more production bake time. Anything in Hamilton in the `experimental` package, should be considered changeable, i.e. their APIs might change, but we'll endeavor to ensure backwards compatible changes when they can be accommodated.

### Ray - Experimental!

[Ray](https://ray.io) is a system to scale python workloads. Hamilton makes it very easy for you to use Ray.

See [https://github.com/stitchfix/hamilton/tree/main/examples/ray](https://github.com/stitchfix/hamilton/tree/main/examples/ray) for an example of using Ray.

#### Single Machine:

Ray is a very easy way to enable multi-processing on a single machine. This enables you to easily make use of multiple CPU cores.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine.

#### Distributed Computation:

If you have a Ray cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel compute, and the potential to scale to large data set sizes, however, you'll be limited to the size of a single machine in terms of the amount of data it can process.

### Dask - Experimental!

[Ray](https://ray.io) is a system to scale python workloads. Hamilton makes it very easy for you to use Ray.

See [https://github.com/stitchfix/hamilton/tree/main/examples/dask](https://github.com/stitchfix/hamilton/tree/main/examples/dask) for an example of using Dask to scale Hamilton computation.

#### Single Machine:

[Dask](https://dask.org) is a very easy way to enable multi-processing on a single machine. This enables you to easily make use of multiple CPU cores.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine.

#### Distributed Computation:

If you have a Dask cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel compute, and the ability to scale to petabyte scale data set sizes.&#x20;

### Koalas on Spark, a.k.a. Pandas API on Spark - Experimental!

[Spark](https://spark.apache.org) is a scalable data processing framework. [Koalas](https://koalas.readthedocs.io/en/latest) was the project code name to implement the [Pandas API on top of Spark](https://spark.apache.org/docs/latest/api/python/user\_guide/pandas\_on\_spark/index.html). Hamilton makes it very easy for you to use Koalas on Spark.

See [https://github.com/stitchfix/hamilton/tree/main/examples/spark](https://github.com/stitchfix/hamilton/tree/main/examples/spark)for an example of using Koalas on Spark to scale Hamilton computation.

#### Single Machine:

You will very likely not want to use Spark on a single machine. It does enable multi-processing, but is likely inferior to Ray or Dask.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine.

#### Distributed Computation:

If you have a Spark cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel compute, and the ability to scale to petabyte scale data set sizes.

## Customizing what Hamilton Returns

