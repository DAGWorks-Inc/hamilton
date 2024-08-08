import pandas as pd

from hamilton import driver

from . import functions


def lambda_handler(event, context):
    df = pd.DataFrame(**event["body"])

    dr = driver.Driver({}, functions)

    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
    ]

    df_result = dr.execute(output_columns, inputs={"input_table": df})

    return {"statusCode": 200, "body": df_result.to_json(orient="split")}
