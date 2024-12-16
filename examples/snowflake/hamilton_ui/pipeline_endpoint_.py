import json
import logging
import os
import sys

from flask import Flask, make_response, request

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVICE_PORT = os.getenv("SERVER_PORT", 8080)
CHARACTER_NAME = os.getenv("CHARACTER_NAME", "I")


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(name)s [%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


logger = get_logger("echo-service")

app = Flask(__name__)


@app.get("/healthcheck")
def readiness_probe():
    return "OK"


@app.post("/echo")
def echo():
    """
    Main handler for input data sent by Snowflake.
    """
    message = request.json
    logger.debug(f"Received request: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    # input format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...],
    #     ...
    #   ]}
    input_rows = message["data"]
    logger.info(f"Received {len(input_rows)} rows")

    # output format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...}],
    #     ...
    #   ]}
    output_rows = [[row[0], get_response(row[1], row[2])] for row in input_rows]
    logger.info(f"Produced {len(output_rows)} rows")

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response


import pandas as pd

# We add this to speed up running things if you have a lot in your python environment.
from hamilton import registry

registry.disable_autoload()
import my_functions  # we import the module here!

from hamilton import driver
from hamilton_sdk import adapters


def get_response(prj_id, output_columns):
    tracker = adapters.HamiltonTracker(
        project_id=prj_id,
        username="admin",
        dag_name="MYDAG",
        tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
    )
    # Instantiate a common spine for your pipeline
    index = pd.date_range("2022-01-01", periods=6, freq="w")
    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        # Note: these do not have to be all series, they could be scalar inputs.
        "signups": pd.Series([1, 10, 50, 100, 200, 400], index=index),
        "spend": pd.Series([10, 10, 20, 40, 40, 50], index=index),
    }
    dr = (
        driver.Builder()
        .with_config({})  # we don't have any configuration or invariant data for this example.
        .with_modules(
            my_functions
        )  # we need to tell hamilton where to load function definitions from
        .with_adapters(tracker)  # we want a pandas dataframe as output
        .build()
    )
    # we need to specify what we want in the final dataframe (these could be function pointers).
    output_columns = ["spend", "signups", "spend_std_dev", "spend_zero_mean_unit_variance"]
    # let's create the dataframe!
    df = dr.execute(output_columns, inputs=initial_columns)
    # `pip install sf-hamilton[visualization]` earlier you can also do
    # dr.visualize_execution(output_columns,'./my_dag.png', {})
    print(df)

    serializable_df = {}

    for key, value in df.items():
        if isinstance(value, pd.Series):
            # Convert Series to dict (or .tolist() for just values)
            serializable_df[key] = {str(k): v for k, v in value.to_dict().items()}
        else:
            # Pass other values as is
            serializable_df[key] = value

    # Serialize to JSON
    print(serializable_df)
    return serializable_df
    return json.dumps(serializable_df, indent=4)
    # return json.dumps(df, indent=4)


if __name__ == "__main__":
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
