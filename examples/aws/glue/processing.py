import sys

import pandas as pd

# awsglue is installed in the AWS Glue worker environment
from awsglue.utils import getResolvedOptions

from hamilton import driver
from hamilton_functions import functions

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["input-table", "output-table"])

    df = pd.read_csv(args["input_table"])

    dr = driver.Driver({}, functions)

    inputs = {"input_table": df}

    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
    ]

    # DAG execution
    df_result = dr.execute(output_columns, inputs=inputs)
    df_result.to_csv(args["output_table"])
