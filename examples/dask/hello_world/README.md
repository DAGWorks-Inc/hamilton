# Hamilton on Dask

Here we have a hello world example showing how you can
take some Hamilton functions and then easily run them
in a distributed setting via dask.

`pip install sf-hamilton[dask-complete]`  or `pip install sf-hamilton dask[complete]` to for the right dependencies to run this example.

File organization:

* `business_logic.py` houses logic that should be invariant to how hamilton is executed.
* `data_loaders.py` houses logic to load data for the business_logic.py module. The
idea is that you'd swap this module out for other ways of loading data.
*  `run.py` is the script that ties everything together.

# Visualization of execution
Here is the graph of execution:

![hello_world_dask](hello_world_dask.png)
