---
description: Here we list available graph adapters
---

# Available Graph Adapters

Use `from hamilton import base` to use these Graph Adapters:

|                                                                                                                 |                                                                                                                                                                                                         |                                                                                                                                                                                                                      |
| --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Name**                                                                                                        | **What it does**                                                                                                                                                                                        | **When you'd use it**                                                                                                                                                                                                |
| base.[SimplePythonDataFrameGraphAdapter](https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L134) | This executes the Hamilton dataflow locally on a machine in a single threaded, single process fashion. It assumes a pandas dataframe as a result.                                                       | This is the default GraphAdapter that Hamilton uses. Use this when you want to execute on a single machine, without parallelization, and you want a pandas dataframe as output.                                      |
| base.[SimplePythonGraphAdapter](https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L149)          | This executes the Hamilton dataflow locally on a machine in a single threaded, single process fashion. It allows you to specify a ResultBuilder to control the return type of what `execute()` returns. | This is the default GraphAdapter that Hamilton uses. Use this when you want to execute on a single machine, without parallelization, and you want to control the return type of the object that `execute()` returns. |

Use `from hamilton.experimental import h_[NAME]`:

|           |                  |                       |
| --------- | ---------------- | --------------------- |
| **Name**  | **What it does** | **When you'd use it** |
| h\_dask.  |                  |                       |
| h\_ray.   |                  |                       |
| h\_spark. |                  |                       |
