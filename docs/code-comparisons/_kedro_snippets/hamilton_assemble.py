# run.py
from hamilton import driver
import dataflow  # module containing node definitions

# pass the module to the `Builder` to create a `Driver`
dr = driver.Builder().with_modules(dataflow).build()