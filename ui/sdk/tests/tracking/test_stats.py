import sys
from collections import namedtuple
from typing import NamedTuple

import pandas as pd
from hamilton_sdk.tracking import data_observation


def test_compute_stats_namedtuple():
    config = namedtuple("config", ["secret_key"])
    result = config("secret_value")
    actual = data_observation.compute_stats(result, "test_node", {})
    assert actual == {
        "observability_type": "unsupported",
        "observability_value": {
            "unsupported_type": str(type(result)),
            "action": "reach out to the DAGWorks team to add support for this type.",
        },
        "observability_schema_version": "0.0.1",
    }


def test_compute_stats_dict():
    actual = data_observation.compute_stats({"a": 1}, "test_node", {})
    assert actual == {
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(dict())),
            "value": {"a": 1},
        },
        "observability_schema_version": "0.0.2",
    }


def test_compute_stats_tuple_dataloader():
    """tests case of a dataloader"""
    actual = data_observation.compute_stats(
        (
            pd.DataFrame({"a": [1, 2, 3]}),
            {"SQL_QUERY": "SELECT * FROM FOO.BAR.TABLE", "CONNECTION_INFO": {"URL": "SOME_URL"}},
        ),
        "test_node",
        {"hamilton.data_loader": True},
    )
    # in python 3.11 numbers change
    if sys.version_info >= (3, 11):
        index = 132
        memory = 156
    else:
        index = 128
        memory = 152
    assert actual == {
        "observability_schema_version": "0.0.2",
        "observability_type": "dict",
        "observability_value": {
            "type": "<class 'dict'>",
            "value": {
                "CONNECTION_INFO": {"URL": "SOME_URL"},
                "DF_MEMORY_BREAKDOWN": {"Index": index, "a": 24},
                "DF_MEMORY_TOTAL": memory,
                "QUERIED_TABLES": {
                    "table-0": {"catalog": "FOO", "database": "BAR", "name": "TABLE"}
                },
                "SQL_QUERY": "SELECT * FROM FOO.BAR.TABLE",
            },
        },
    }


def test_compute_states_tuple_namedtuple():
    """Tests namedtuple capture correctly"""

    class Foo(NamedTuple):
        x: int
        y: str

    f = Foo(1, "a")
    actual = data_observation.compute_stats(
        f,
        "test_node",
        {"some_tag": "foo-bar"},
    )
    assert actual == {
        "observability_schema_version": "0.0.2",
        "observability_type": "dict",
        "observability_value": {
            "type": "<class 'dict'>",
            "value": {
                "x": 1,
                "y": "a",
            },
        },
    }
