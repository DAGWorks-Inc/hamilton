from collections import namedtuple

import pandas as pd
from hamilton_sdk.tracking import stats


def test_compute_stats_namedtuple():
    config = namedtuple("config", ["secret_key"])
    result = config("secret_value")
    actual = stats.compute_stats(result, "test_node", {})
    assert actual == {
        "observability_type": "unsupported",
        "observability_value": {
            "unsupported_type": str(type(result)),
            "action": "reach out to the DAGWorks team to add support for this type.",
        },
        "observability_schema_version": "0.0.1",
    }


def test_compute_stats_dict():
    actual = stats.compute_stats({"a": 1}, "test_node", {})
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
    actual = stats.compute_stats(
        (
            pd.DataFrame({"a": [1, 2, 3]}),
            {"SQL_QUERY": "SELECT * FROM FOO.BAR.TABLE", "CONNECTION_INFO": {"URL": "SOME_URL"}},
        ),
        "test_node",
        {"hamilton.data_loader": True},
    )
    assert actual == {
        "observability_schema_version": "0.0.2",
        "observability_type": "dict",
        "observability_value": {
            "type": "<class 'dict'>",
            "value": {
                "CONNECTION_INFO": {"URL": "SOME_URL"},
                "DF_MEMORY_BREAKDOWN": {"Index": 128, "a": 24},
                "DF_MEMORY_TOTAL": 152,
                "QUERIED_TABLES": {
                    "table-0": {"catalog": "FOO", "database": "BAR", "name": "TABLE"}
                },
                "SQL_QUERY": "SELECT * FROM FOO.BAR.TABLE",
            },
        },
    }
