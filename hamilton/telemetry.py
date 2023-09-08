"""
This module contains code that relates to sending Hamilton usage telemetry.

To disable sending telemetry there are three ways:

1. Set it to false programmatically in your driver:
  >>> from hamilton import telemetry
  >>> telemetry.disable_telemetry()
2. Set it to `false` in ~/.hamilton.conf under `DEFAULT`
  [DEFAULT]
  telemetry_enabled = True
3. Set HAMILTON_TELEMETRY_ENABLED=false as an environment variable:
  HAMILTON_TELEMETRY_ENABLED=false python run.py
  or:
  export HAMILTON_TELEMETRY_ENABLED=false
"""
import configparser
import json
import logging
import os
import platform
import threading
import traceback
import uuid
from typing import Dict, Optional
from urllib import request

try:
    from . import base
    from .version import VERSION
except ImportError:
    from version import VERSION

    from hamilton import base

logger = logging.getLogger(__name__)

STR_VERSION = ".".join([str(i) for i in VERSION])
HOST = "https://app.posthog.com"
TRACK_URL = f"{HOST}/capture/"  # https://posthog.com/docs/api/post-only-endpoints
API_KEY = "phc_mZg8bkn3yvMxqvZKRlMlxjekFU5DFDdcdAsijJ2EH5e"
START_EVENT = "os_hamilton_run_start"
END_EVENT = "os_hamilton_run_end"
DRIVER_FUNCTION = "os_hamilton_driver_function_call"
DATAFLOW_FUNCTION = "os_hamilton_dataflow_function_call"
DATAFLOW_DOWNLOAD = "os_hamilton_dataflow_download_call"
TIMEOUT = 2
MAX_COUNT_SESSION = 1000

DEFAULT_CONFIG_LOCATION = os.path.expanduser("~/.hamilton.conf")


def _load_config(config_location: str) -> configparser.ConfigParser:
    """Pulls config. Gets/sets default anonymous ID.

    Creates the anonymous ID if it does not exist, writes it back if so.
    :param config_location: location of the config file.
    """
    config = configparser.ConfigParser()
    try:
        with open(config_location) as f:
            config.read_file(f)
    except Exception:
        config["DEFAULT"] = {}
    else:
        if "DEFAULT" not in config:
            config["DEFAULT"] = {}

    if "anonymous_id" not in config["DEFAULT"]:
        config["DEFAULT"]["anonymous_id"] = str(uuid.uuid4())
        try:
            with open(config_location, "w") as f:
                config.write(f)
        except Exception:
            pass
    return config


def _check_config_and_environ_for_telemetry_flag(
    telemetry_default: bool, config_obj: configparser.ConfigParser
):
    """Checks the config and environment variables for the telemetry value.

    Note: the environment variable has greater precedence than the config value.
    """
    telemetry_enabled = telemetry_default
    if "telemetry_enabled" in config_obj["DEFAULT"]:
        try:
            telemetry_enabled = config_obj.getboolean("DEFAULT", "telemetry_enabled")
        except ValueError as e:
            logger.debug(
                "Unable to parse value for `telemetry_enabled` from config. " f"Encountered {e}"
            )
    if os.environ.get("HAMILTON_TELEMETRY_ENABLED") is not None:
        env_value = os.environ.get("HAMILTON_TELEMETRY_ENABLED")
        # set the value
        config_obj["DEFAULT"]["telemetry_enabled"] = env_value
        try:
            telemetry_enabled = config_obj.getboolean("DEFAULT", "telemetry_enabled")
        except ValueError as e:
            logger.debug(
                "Unable to parse value for `HAMILTON_TELEMETRY_ENABLED` from environment. "
                f"Encountered {e}"
            )
    return telemetry_enabled


config = _load_config(DEFAULT_CONFIG_LOCATION)
g_telemetry_enabled = _check_config_and_environ_for_telemetry_flag(True, config)
g_anonymous_id = config["DEFAULT"]["anonymous_id"]
call_counter = 0


def disable_telemetry():
    """Disables telemetry tracking."""
    global g_telemetry_enabled
    g_telemetry_enabled = False


def is_telemetry_enabled() -> bool:
    """Returns whether telemetry tracking is enabled or not.

    Increments a counter to stop sending telemetry after 1000 invocations.
    """
    if g_telemetry_enabled:
        global call_counter
        if call_counter == 0:
            # Log only the first time someone calls this function; don't want to spam them.
            logger.warning(
                "Note: Hamilton collects completely anonymous data about usage. "
                "This will help us improve Hamilton over time. "
                "See https://github.com/dagworks-inc/hamilton#usage-analytics--data-privacy for details."
            )
        call_counter += 1
        if call_counter > MAX_COUNT_SESSION:
            # we have hit our limit -- disable telemetry.
            return False
        return True
    else:
        return False


# base properties to instantiate on module load.
BASE_PROPERTIES = {
    "os_type": os.name,
    "os_version": platform.platform(),
    "python_version": f"{platform.python_version()}/{platform.python_implementation()}",
    "distinct_id": g_anonymous_id,
    "hamilton_version": list(VERSION),
    "telemetry_version": "0.0.1",
}


def create_start_event_json(
    number_of_nodes: int,
    number_of_modules: int,
    number_of_config_items: int,
    decorators_used: Dict[str, int],
    graph_adapter_used: str,
    result_builder_used: str,
    driver_run_id: uuid.UUID,
    error: Optional[str],
    graph_executor_class: str,
):
    """Creates the start event JSON.

    The format we want to follow is the one for [post-hog](# https://posthog.com/docs/api/post-only-endpoints).

    :param number_of_nodes: the number of nodes in the graph
    :param number_of_modules: the number of modules parsed
    :param number_of_config_items: the number of items in configuration
    :param decorators_used: a dict of decorator -> count
    :param graph_adapter_used: the name of the graph adapter used
    :param result_builder_used: the name of the result builder used
    :param driver_run_id: the ID of the run
    :param error: an error string if any
    :param driver_class: the name of the driver class used to call this
    :return: dictionary to send.
    """
    event = {
        "api_key": API_KEY,
        "event": START_EVENT,
        "properties": {},
    }
    event["properties"].update(BASE_PROPERTIES)
    payload = {
        "number_of_nodes": number_of_nodes,  # approximately how many nodes were in the DAG?
        "number_of_modules": number_of_modules,  # approximately how many modules were used?
        "number_of_config_items": number_of_config_items,  # how many configs are people passing in?
        "decorators_used": decorators_used,  # what decorators were used, and how many times?
        "graph_adapter_used": graph_adapter_used,  # what was the graph adapter used?
        "result_builder_used": result_builder_used,  # what was the result builder used?
        "driver_run_id": str(driver_run_id),  # was this a new driver object? or?
        "error": error,  # if there was an error, what was the trace? (limited to Hamilton code)
        "graph_executor_class": graph_executor_class,  # what driver class was used to call this
    }
    event["properties"].update(payload)
    return event


def create_end_event_json(
    is_success: bool,
    runtime_seconds: float,
    number_of_outputs: int,
    number_of_overrides: int,
    number_of_inputs: int,
    driver_run_id: uuid.UUID,
    error: Optional[str],
):
    """Creates the end event JSON.

    The format we want to follow is the one for [post-hog](# https://posthog.com/docs/api/post-only-endpoints).

    :param is_success: whether execute was successful
    :param runtime_seconds: how long execution took
    :param number_of_outputs: the number of outputs requested
    :param number_of_overrides: the number of overrides provided
    :param number_of_inputs: the number of inputs provided
    :param driver_run_id: the run ID of this driver run
    :param error: the error string if any
    :return: dictionary to send.
    """
    event = {
        "api_key": API_KEY,
        "event": END_EVENT,
        "properties": {},
    }
    event["properties"].update(BASE_PROPERTIES)
    payload = {
        "is_success": is_success,  # was this run successful?
        "runtime_seconds": runtime_seconds,  # how long did it take
        "number_of_outputs": number_of_outputs,  # how many outputs were requested
        "number_of_overrides": number_of_overrides,  # how many outputs were requested
        "number_of_inputs": number_of_inputs,  # how many user provided things are there
        "driver_run_id": str(driver_run_id),  # let's tie this to a particular driver instantiation
        "error": error,  # if there was an error, what was the trace? (limited to Hamilton code)
    }
    event["properties"].update(payload)
    return event


def create_driver_function_invocation_event(function_name: str) -> dict:
    """Function to create payload for tracking function name invocation.

    :param function_name: the name of the driver function
    :return: dict representing the JSON to send.
    """
    event = {
        "api_key": API_KEY,
        "event": DRIVER_FUNCTION,
        "properties": {},
    }
    event["properties"].update(BASE_PROPERTIES)
    payload = {
        "function_name": function_name,  # what was the name of the driver function?
    }
    event["properties"].update(payload)
    return event


def create_dataflow_function_invocation_event_json(canonical_function_name: str) -> dict:
    """Function that creates JSON to track dataflow module function calls.

    :param canonical_function_name: the name of the function in the dataflow module.
    :return: the dictionary representing the event.
    """
    event = {
        "api_key": API_KEY,
        "event": DATAFLOW_FUNCTION,
        "properties": {},
    }
    event["properties"].update(BASE_PROPERTIES)
    payload = {
        "function_name": canonical_function_name,  # what was the name of the driver function?
    }
    event["properties"].update(payload)
    return event


def create_dataflow_download_event_json(
    category: str, user: str, dataflow_name: str, version: str
) -> dict:
    """Function that creates JSON to track dataflow download calls.

    :param category: the category of the dataflow. OFFICIAL or USER.
    :param user: the user's github handle, if applicable.
    :param dataflow_name: the name of the dataflow.
    :param version: the git commit version of the dataflow, OR the sf-hamilton-contrib package version.
    :return: dictionary representing the event.
    """
    event = {
        "api_key": API_KEY,
        "event": DATAFLOW_DOWNLOAD,
        "properties": {},
    }
    event["properties"].update(BASE_PROPERTIES)
    _category = "OFFICIAL" if category == "OFFICIAL" else "USER"

    payload = {
        "category": _category,
        "dataflow_name": dataflow_name,
        "commit_version": version,
    }
    if _category == "USER":
        payload["github_user"] = user
    event["properties"].update(payload)
    return event


def create_and_send_contrib_use(module_name: str, version: str):
    """Function to send contrib module use -- this is used from the contrib package.

    :param module_name: the name of the module
    :param version: the package version.
    """
    if module_name == "__main__" or module_name == "__init__":
        return
    try:
        parts = module_name.split(".")
        if "official" in parts:
            category = "OFFICIAL"
        else:
            category = "USER"
            user = parts[-2]
        dataflow = parts[-1]
        version = "sf-contrib-" + ".".join(map(str, version))
        event_json = create_dataflow_download_event_json(category, user, dataflow, version)
    except Exception as e:
        # capture any exception!
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Encountered error while constructing create_and_send_contrib_use json:\n{e}"
            )
    else:
        send_event_json(event_json)


def _send_event_json(event_json: dict):
    """Internal function to send the event JSON to posthog.

    :param event_json: the dictionary of data to JSON serialize and send
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": "TODO",
        "User-Agent": f"hamilton/{STR_VERSION}",
    }
    try:
        data = json.dumps(event_json).encode()
        req = request.Request(TRACK_URL, data=data, headers=headers)
        with request.urlopen(req, timeout=TIMEOUT) as f:
            res = f.read()
            if f.code != 200:
                raise RuntimeError(res)
    except Exception as e:
        if logger.isEnabledFor(logging.DEBUG):
            logging.debug(f"Failed to send telemetry data: {e}")
    else:
        if logger.isEnabledFor(logging.DEBUG):
            logging.debug(f"Succeed in sending telemetry consisting of [{data}].")


def send_event_json(event_json: dict):
    """Sends the event json in its own thread.

    :param event_json: the data to send
    """
    if not g_telemetry_enabled:
        raise RuntimeError("Won't send; tracking is disabled!")
    try:
        th = threading.Thread(target=_send_event_json, args=(event_json,))
        th.start()
    except Exception as e:
        # capture any exception!
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Encountered error while sending event JSON via it's own thread:\n{e}")


def sanitize_error(exc_type, exc_value, exc_traceback) -> str:
    """Sanitizes an incoming error and pulls out a string to tell us where it came from.

    :param exc_type: pulled from `sys.exc_info()`
    :param exc_value: pulled from `sys.exc_info()`
    :param exc_traceback: pulled from `sys.exc_info()`
    :return: string to use for telemetry
    """
    try:
        te = traceback.TracebackException(exc_type, exc_value, exc_traceback, limit=-5)
        sanitized_string = ""
        for stack_item in te.stack:
            stack_file_path = stack_item.filename.split(os.sep)
            # take last 4 places only -- that's how deep hamilton is.
            stack_file_path = stack_file_path[-4:]
            try:
                # find first occurrence
                index = stack_file_path.index("hamilton")
            except ValueError:
                sanitized_string += "...<USER_CODE>...\n"
                continue
            file_name = "..." + "/".join(stack_file_path[index:])
            sanitized_string += f"{file_name}, line {stack_item.lineno}, in {stack_item.name}\n"
        return sanitized_string
    except Exception as e:
        # we don't want this to fail
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Encountered exception sanitizing error. Got:\n{e}")
        return "FAILED_TO_SANITIZE_ERROR"


def get_adapter_name(adapter: base.HamiltonGraphAdapter) -> str:
    """Get the class name of the `hamilton` adapter used.

    If we detect it's not a Hamilton one, we do not track it.

    :param adapter: base.HamiltonGraphAdapter object.
    :return: string modeul + class name of the adapter.
    """
    # Check whether it's a hamilton based adapter
    if adapter.__module__.startswith("hamilton."):
        adapter_name = f"{adapter.__module__}.{adapter.__class__.__name__}"
    else:
        adapter_name = "custom_adapter"
    return adapter_name


def get_result_builder_name(adapter: base.HamiltonGraphAdapter) -> str:
    """Get the class name of the `hamilton` result builder used.

    If we detect it's not a base one, we do not track it.

    :param adapter: base.HamiltonGraphAdapter object.
    :return: string module + class name of the result builder.
    """
    class_to_inspect = adapter
    # if there is an attribute, get that out to use as the class to inspect
    if hasattr(adapter, "result_builder"):
        class_to_inspect = getattr(adapter, "result_builder")
    # Go by class itself
    if isinstance(class_to_inspect, base.StrictIndexTypePandasDataFrameResult):
        result_builder_name = "hamilton.base.StrictIndexTypePandasDataFrameResult"
    elif isinstance(class_to_inspect, base.PandasDataFrameResult):
        result_builder_name = "hamilton.base.PandasDataFrameResult"
    elif isinstance(class_to_inspect, base.DictResult):
        result_builder_name = "hamilton.base.DictResult"
    elif isinstance(class_to_inspect, base.NumpyMatrixResult):
        result_builder_name = "hamilton.base.NumpyMatrixResult"
    else:
        result_builder_name = "custom_builder"
    return result_builder_name
