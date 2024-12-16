import logging
import os
import sys

import pandas as pd
from flask import Flask, make_response, request

from hamilton import registry

registry.disable_autoload()
import my_functions  # we import the module here!

from hamilton import driver
from hamilton_sdk import adapters

# WRAPER CODE FOR SNOWFLAKE FUNCTION ######

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
    message = request.json
    logger.debug(f"Received request: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    input_rows = message["data"]
    logger.info(f"Received {len(input_rows)} rows")

    output_rows = [[row[0], get_response(row[1], row[2], row[3], row[4])] for row in input_rows]
    logger.info(f"Produced {len(output_rows)} rows")

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response


# END OF WRAPER CODE FOR SNOWFLAKE FUNCTION ######


def get_response(prj_id, spend, signups, output_columns):
    tracker = adapters.HamiltonTracker(
        project_id=prj_id,
        username="admin",
        dag_name="MYDAG",
        tags={"environment": "R&D", "team": "MY_TEAM", "version": "Beta"},
    )
    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        "signups": pd.Series(spend),
        "spend": pd.Series(signups),
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

    df = dr.execute(output_columns, inputs=initial_columns)

    serializable_df = {}

    for key, value in df.items():
        if isinstance(value, pd.Series):
            # Convert Series to dict (or .tolist() for just values)
            serializable_df[key] = {str(k): v for k, v in value.to_dict().items()}
        else:
            # Pass other values as is
            serializable_df[key] = value

    return serializable_df


if __name__ == "__main__":
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
