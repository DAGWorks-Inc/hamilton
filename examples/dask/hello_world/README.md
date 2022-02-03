# Hamilton on Dask

Here we have a hello world example showing how you can
take some Hamilton functions and then easily run them
in a distributed setting via dask.

File organization:

* `business_logic.py` houses logic that should be invariant to how hamilton is executed.
* `data_loaders.py` houses logic to load data for the business_logic.py module. The
idea is that you'd swap this module out for other ways of loading data.
*  `run.py` is the script that ties everything together.
