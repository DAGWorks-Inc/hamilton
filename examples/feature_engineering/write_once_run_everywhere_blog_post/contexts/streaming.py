import json
import logging
import pathlib

from bytewax.connectors.files import CSVInput, FileOutput
from bytewax.dataflow import Dataflow
from components import aggregations, data_loaders, features, joins, model

from hamilton import base, driver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = str(json.loads(key_bytes)) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return key, payload


def serialize_with_key(key_payload):
    key, payload = key_payload
    return json.dumps(key).encode("utf-8"), json.dumps(payload).encode("utf-8")


dr = driver.Driver(
    {"mode": "streaming"},
    aggregations,
    data_loaders,
    joins,
    features,
    model,
    adapter=base.DefaultAdapter(),
)


def hamilton_predict(payload: dict):
    series_out = dr.execute(["predictions"], inputs={"survey_event": payload})["predictions"]
    return {"prediction": series_out.values[0], "client_id": payload["client_id"]}


flow = Dataflow()
flow.input("surveys-in", CSVInput(pathlib.Path("streaming.csv")))
flow.inspect(logger.info)

flow.map(deserialize)

flow.map(hamilton_predict)

flow.map(serialize_with_key)

flow.output("surveys-out", FileOutput(pathlib.Path("streaming.out")))
