import numpy as np  # noqa: F401
import pandas as pd  # noqa: F401
from sklearn.linear_model import LinearRegression  # noqa: F401
from sklearn.metrics import mean_absolute_error  # noqa: F401
from sklearn.preprocessing import StandardScaler  # noqa: F401

# Write 4 functions

# 1. load/create the pandas dataframe

# 2. Create the data_sets

# 3. Create the linear model

# 4. Evaluate the model


if __name__ == "__main__":
    import __main__
    from hamilton import driver

    dr = driver.Builder().with_modules(__main__).build()
    dr.display_all_functions("mpg_pipeline.png")
    result = dr.execute(["evaluated_model", "linear_model"])
    print(result)
