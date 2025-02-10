from hamilton import telemetry

# disable telemetry for all tests!
telemetry.disable_telemetry()

# dask_expr got made default, except for python 3.9 and below
import sys

if sys.version_info < (3, 10):
    import dask

    dask.config.set({"dataframe.query-planning": False})
