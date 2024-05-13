import json
from functools import singledispatch
from typing import Any, Dict

import pandas as pd
from hamilton_sdk.tracking import sql_utils


@singledispatch
def compute_stats(result, node_name: str, node_tags: dict) -> Dict[str, Any]:
    """This is the default implementation for computing stats on a result.

    All other implementations should be registered with the `@compute_stats.register` decorator.

    :param result:
    :param node_name:
    :param node_tags:
    :return:
    """
    return {
        "observability_type": "unsupported",
        "observability_value": {
            "unsupported_type": str(type(result)),
            "action": "reach out to the DAGWorks team to add support for this type.",
        },
        "observability_schema_version": "0.0.1",
    }


@compute_stats.register(str)
@compute_stats.register(int)
@compute_stats.register(float)
@compute_stats.register(bool)
def compute_stats_primitives(result, node_name: str, node_tags: dict) -> Dict[str, Any]:
    return {
        "observability_type": "primitive",
        "observability_value": {
            "type": str(type(result)),
            "value": result,
        },
        "observability_schema_version": "0.0.1",
    }


@compute_stats.register(dict)
def compute_stats_dict(result: dict, node_name: str, node_tags: dict) -> Dict[str, Any]:
    """call summary stats on the values in the dict"""
    try:
        # if it's JSON serializable, take it.
        json.dumps(result)
        result_values = result
    except Exception:
        result_values = {}
        for k, v in result.items():
            # go through each value
            if isinstance(v, (str, int, float, bool)):
                result_values[k] = v
                continue
            # else it's a dict, list, tuple, etc. Compute stats.
            v_result = compute_stats(v, node_name, node_tags)
            # determine what to pull out of the result for the value
            observed_type = v_result["observability_type"]
            if observed_type == "primitive":
                result_values[k] = v
            elif observed_type == "unsupported":
                str_value = str(v)
                # else just string it -- max 200 chars.
                if len(str_value) > 200:
                    str_value = str_value[:200] + "..."
                result_values[k] = str_value
            else:
                # it's a DF, Series -- so take full result.
                result_values[k] = v_result["observability_value"]

    return {
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(result)),
            "value": result_values,
        },
        "observability_schema_version": "0.0.2",
    }


@compute_stats.register(tuple)
def compute_stats_tuple(result: tuple, node_name: str, node_tags: dict) -> Dict[str, Any]:
    if "hamilton.data_loader" in node_tags and node_tags["hamilton.data_loader"] is True:
        # assumption it's a tuple
        if isinstance(result[1], dict):
            try:
                # double check that it's JSON serializable
                raw_data = json.dumps(result[1])
                _metadata = json.loads(raw_data)
            except Exception:
                _metadata = str(result[1])
                if len(_metadata) > 1000:
                    _metadata = _metadata[:1000] + "..."
            else:
                # enrich it
                if (
                    "SQL_QUERY" in _metadata
                ):  # we might need to think how to make this a constant...
                    _metadata["QUERIED_TABLES"] = sql_utils.parse_sql_query(_metadata["SQL_QUERY"])
                    if isinstance(result[0], pd.DataFrame):
                        # TODO: move this to dataframe stats collection
                        _memory = result[0].memory_usage(deep=True)
                        _metadata["DF_MEMORY_TOTAL"] = int(_memory.sum())
                        _metadata["DF_MEMORY_BREAKDOWN"] = _memory.to_dict()
            return {
                "observability_type": "dict",
                "observability_value": {
                    "type": str(type(result[1])),
                    "value": _metadata,
                },
                "observability_schema_version": "0.0.2",
            }
    # namedtuple -- this how we guide people to not have something tracked easily.
    # so we skip it if it has a `secret_key`. This is hacky -- better choice would
    # be to have an internal object or way to decorate a parameter to not track it.
    if hasattr(result, "_asdict") and not hasattr(result, "secret_key"):
        return compute_stats_dict(result._asdict(), node_name, node_tags)
    return {
        "observability_type": "unsupported",
        "observability_value": {
            "unsupported_type": str(type(result)),
            "action": "reach out to the DAGWorks team to add support for this type.",
        },
        "observability_schema_version": "0.0.1",
    }


@compute_stats.register(list)
def compute_stats_list(result: list, node_name: str, node_tags: dict) -> Dict[str, Any]:
    """call summary stats on the values in the list"""
    try:
        # if it's JSON serializable, take it.
        json.dumps(result)
        result_values = result
    except Exception:
        result_values = []
        for v in result:
            if isinstance(v, (list, dict, tuple)):
                try:
                    json.dumps(v)
                except Exception:
                    v = str(v)
                    # else just string it -- max 200 chars.
                    if len(v) > 200:
                        v = v[:200] + "..."
            else:
                v_result = compute_stats(v, node_name, node_tags)
                # determine what to pull out of the result for the value
                observed_type = v_result["observability_type"]
                if observed_type == "dagworks_describe":
                    # it's a DF, Series -- so take full result.
                    v = v_result["observability_value"]
                elif observed_type == "unsupported":
                    v = str(v)
                    # else just string it -- max 200 chars.
                    if len(v) > 200:
                        v = v[:200] + "..."
                elif observed_type == "dict":
                    v = v_result["observability_value"]
            result_values.append(v)
    return {
        # yes dict type -- that's so that we can display in the UI. It's a hack.
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(result)),
            "value": result_values,
        },
        "observability_schema_version": "0.0.2",
    }
