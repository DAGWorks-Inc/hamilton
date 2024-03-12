import dask

from hamilton import telemetry

# disable telemetry for all tests!
telemetry.disable_telemetry()

# required until we fix the DataFrameResultBuilder to work with dask-expr
dask.config.set({"dataframe.query-planning": False})
