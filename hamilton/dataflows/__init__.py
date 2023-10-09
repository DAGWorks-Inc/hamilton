"""
This module houses functions and tools to interact with off-the-shelf
dataflows.

Probably want this to be a CLI too. -- e.g. init would make more sense.
"""
import functools
import importlib
import json
import logging
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from types import ModuleType
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union

from hamilton import driver, telemetry

logger = logging.getLogger(__name__)

"""
Paths that we care about.
Assumptions:
 - ~/.hamilton/dataflows/ is where we store dataflows. This is fine because we should be able to save locally.
 - This will act as a cache for dataflows.
 - People can modify this location globally.
 - Directory should mirror github (e.g. contrib/hamilton/contrib/user/{user}/{dataflow})
 - We don't have to deal with the python path because this module knows how to import it.

TODOs:
 - finish init
 - make sure it works on windows
 - make functions more robust (see comments in functions).
"""
COMMON_PATH = "{commit_ish}/contrib/hamilton/contrib"
BASE_URL = f"https://raw.githubusercontent.com/dagworks-inc/hamilton/{COMMON_PATH}"
DATAFLOW_FOLDER = os.path.expanduser("~/.hamilton/dataflows")
USER_PATH = DATAFLOW_FOLDER + "/" + COMMON_PATH + "/user/{user}/{dataflow}"
OFFICIAL_PATH = DATAFLOW_FOLDER + "/" + COMMON_PATH + "/official/{dataflow}"


def _track_function_call(call_fn: Callable) -> Callable:
    """Decorator to wrap the __call__ to count usage.

    :param call_fn: the `__call__` function.
    :return: the wrapped call function.
    """

    @functools.wraps(call_fn)
    def track_call(*args, **kwargs):
        event_json = telemetry.create_dataflow_function_invocation_event_json(call_fn.__name__)
        telemetry.send_event_json(event_json)
        return call_fn(*args, **kwargs)

    return track_call


def _track_download(is_official: bool, user: Optional[str], dataflow_name: str, version: str):
    """Inner function to track "downloads" of a dataflow.

    :param is_official: is this an official dataflow? False == user.
    :param user: If not official, what is the github user name.
    :param dataflow_name: the name of the dataflow
    :param version: the version. Either git hash, or the package version.
    """
    if is_official:
        category = "OFFICIAL"
    else:
        category = "USER"
    event_json = telemetry.create_dataflow_download_event_json(
        category, user, dataflow_name, version
    )
    telemetry.send_event_json(event_json)


def _get_request(url: str) -> Tuple[int, str]:
    """Makes a GET request to the given URL and returns the status code and response data.

    :param url: the url to make the request to.
    :return: tuple of status code and text response decoded as utf-8.
    """
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read().decode("utf-8")
            return response.status, data

    except urllib.error.HTTPError as e:
        return e.code, e.reason


@_track_function_call
def clear_storage():
    """Clears all the data under DATAFLOW_FOLDER."""
    if os.path.exists(DATAFLOW_FOLDER):
        shutil.rmtree(DATAFLOW_FOLDER)
        logger.info(f"Cleared storage at {DATAFLOW_FOLDER}")
    else:
        logger.info(f"Folder {DATAFLOW_FOLDER} does not exist.")


# want TTL cache on this call to not get rate limited.
last_time_called = None
last_resolve_value = None


@_track_function_call
def resolve_latest(branch: str = "main") -> str:
    """Resolves
    https://api.github.com/repos/DAGWorks-Inc/hamilton/git/refs/heads/main
    :return: commit for what "main" is.
    """
    global last_time_called
    global last_resolve_value
    if last_time_called is not None and last_resolve_value is not None:
        if (last_time_called + 60.0) > time.time():
            logger.info("using cached resolve_latest")
            return last_resolve_value
    url = f"https://api.github.com/repos/DAGWorks-Inc/hamilton/git/refs/heads/{branch}"
    status_code, text = _get_request(url)
    # response = requests.get(url)
    if status_code != 200:
        raise ValueError(
            f"Failed to resolve latest commit for branch {branch}:\n{status_code}\n{text}"
        )
    commit_sha = json.loads(text)["object"]["sha"]
    last_time_called = time.time()
    last_resolve_value = commit_sha
    return commit_sha


@_track_function_call
def resolve_dataflow_commit(dataflow: str, user: str = None) -> str:
    """Resolves the commit for a dataflow.

    :param dataflow:
    :param user:
    :return:
    """
    if user:
        url = f"https://hub.dagworks.io/commits/Users/{user}/{dataflow}/commit.txt"
    else:
        url = f"https://hub.dagworks.io/commits/Official/{dataflow}/commit.txt"
    # response = requests.get(url)
    status_code, text = _get_request(url)
    if status_code != 200:
        raise ValueError(
            f"Failed to resolve latest commit for {user}/{dataflow}:\n{status_code}\n{text}"
        )
    chunk_index = text.find("[commit::")
    if chunk_index < 0:
        raise ValueError("No commit found")
    commit = text[chunk_index + len("[commit::") :]
    commit_sha = commit[: commit.find("]")]
    return commit_sha


@_track_function_call
def pull_module(dataflow: str, user: str = None, version: str = "latest", overwrite: bool = False):
    """Pulls a dataflow

    Saves to current working directory so that an import should just work right after doing this.
    1. create URL
    2. pull dataflow
    3. save to location

    Warn if overwriting the location.

    :param dataflow: the dataflow name.
    :param user: the user's github handle
    :param version: the commit version. Latest is the default.
    :param overwrite: whether to overwrite.
    :return:
    """
    if version == "latest":
        version = resolve_dataflow_commit(dataflow, user)
    if user:
        _track_download(False, user, dataflow, version)
        logger.info(f"pulling dataflow {user}/{dataflow} with version {version}")
    else:
        _track_download(True, None, dataflow, version)
        logger.info(f"pulling official dataflow {dataflow} with version {version}")

    h_files = ["__init__.py", "requirements.txt", "README.md", "valid_configs.jsonl", "tags.json"]

    local_file_path = USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow)
    if os.path.exists(local_file_path) and not overwrite:
        raise ValueError(
            f"Dataflow {user}/{dataflow} with version {version} already exists locally. Not downloading."
        )
    os.makedirs(local_file_path, exist_ok=True)
    for h_file in h_files:
        url = BASE_URL.format(commit_ish=version) + f"/user/{user}/{dataflow}/{h_file}"
        # response = requests.get(url)
        status_code, text = _get_request(url)
        if status_code == 404:
            raise ValueError(f"Dataflow {user}/{dataflow}/{h_file} not found at {url}.")
        elif status_code != 200:
            raise ValueError(
                f"Dataflow {user}/{dataflow} returned status code {status_code}:\n{text}"
            )
        file_contents = text
        if os.path.exists(os.path.join(local_file_path, h_file)):
            logger.info(f"Warning: overwriting {h_file} in {local_file_path}")
        with open(os.path.join(local_file_path, h_file), "w") as f:
            f.write(file_contents)
            logger.info(f"wrote {h_file} to {local_file_path}")


@_track_function_call
def import_module(
    dataflow: str, user: str = None, version: str = "latest", overwrite: bool = False
) -> ModuleType:
    """Pulls & imports dataflow

    :param dataflow:
    :param user: Optional. If none it assumes official.
    :param version:
    :param overwrite:
    :return:
    """
    if version == "latest":
        version = resolve_dataflow_commit(dataflow, user)
    if user:
        local_file_path = (
            USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow) + "/__init__.py"
        )
    else:
        local_file_path = (
            OFFICIAL_PATH.format(commit_ish=version, dataflow=dataflow) + "/__init__.py"
        )
    if not os.path.exists(local_file_path) or overwrite:
        pull_module(dataflow, user, version=version, overwrite=overwrite)
    else:
        logger.info(f"Found {user}/{dataflow} with version {version} locally. Not downloading.")

    if dataflow in sys.modules:
        logger.info(
            f"Warning: overwriting existing {dataflow} module with version {sys.modules[dataflow].__version__}"
        )
    spec = importlib.util.spec_from_file_location(dataflow, local_file_path)
    module = importlib.util.module_from_spec(spec)
    module.__version__ = version
    sys.modules[dataflow] = module
    spec.loader.exec_module(module)
    return module


class InspectResult(NamedTuple):
    version: str  # git commit sha/package version
    user: str  # github user URL
    dataflow: str  # dataflow URL
    python_dependencies: List[str]  # python dependencies
    configurations: List[str]  # configurations for the dataflow stored as a JSON string


@_track_function_call
def inspect(user: str, dataflow: str, version: str = "latest") -> InspectResult:
    """Inspects the dataflow for information

    - python deps  (people will have to pin/hamilton version things too)
    - inputs
    - nodes
    - designated outputs
    - commit hash
    """
    if version == "latest":
        version = resolve_dataflow_commit(dataflow, user)

    local_file_path = USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow)
    if not os.path.exists(local_file_path):
        raise ValueError(
            f"Dataflow {user}/{dataflow} with version {version} does not exist locally. Not inspecting."
        )
    # return dictionary of python deps, inputs, nodes, designated outputs, commit hash
    info: Dict[str, Union[str, List[Dict], List[str]]] = {
        "version": version,
        "user": f"https://github.com/{user}",
        "dataflow": f"https://github.com/dagworks-inc/hamilton/tree/{version}/contrib/contrib/user/{user}/{dataflow}",
        "python_dependencies": [],
        "configurations": [],
    }

    with open(os.path.join(local_file_path, "requirements.txt"), "r") as f:
        file_contents = [line.strip() for line in f.readlines()]
        info["python_dependencies"] = file_contents

    with open(os.path.join(local_file_path, "valid_configs.jsonl"), "r") as f:
        file_contents = [line.strip() for line in f.readlines()]
        info["configurations"] = file_contents
    return InspectResult(**info)


@_track_function_call
def inspect_module(module: ModuleType) -> dict:
    """Inspects the dataflow for information

    - python deps  (people will have to pin/hamilton version things too)
    - inputs
    - nodes
    - designated outputs
    - commit hash
    """
    if not module.__file__.startswith(DATAFLOW_FOLDER):
        logger.info("not a downloaded dataflow module.")
        return {}
    user = module.__file__.split("/")[-3]
    dataflow = module.__file__.split("/")[-2]
    version = module.__version__

    local_file_path = USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow)
    if not os.path.exists(local_file_path):
        raise ValueError(
            f"Dataflow {user}/{dataflow} with version {version} does not exist locally. Not inspecting."
        )
    # return dictionary of python deps, inputs, nodes, designated outputs, commit hash
    info: Dict[str, Union[str, List[Dict], List[str], List[Tuple[str, Type]]]] = {
        "git-reference/version": version,
        "user": f"https://github.com/{user}",
        "dataflow": f"https://github.com/dagworks-inc/hamilton/tree/{version}/contrib/contrib/user/{user}/{dataflow}",
        "python_dependencies": [],
        "inputs": [],
        "nodes": [],
        "designated_outputs": [],
        "configurations": [],
    }

    with open(os.path.join(local_file_path, "requirements.txt"), "r") as f:
        file_contents = f.readlines()
        info["python_dependencies"] = file_contents

    with open(os.path.join(local_file_path, "valid_configs.jsonl"), "r") as f:
        file_contents = [json.loads(s) for s in f.readlines()]
        info["configurations"] = file_contents

    dr = driver.Driver(info["configurations"][0], module)
    vars = dr.list_available_variables()
    info["inputs"] = [(var.name, var.type) for var in vars if var.is_external_input]
    info["nodes"] = [(var.name, var.type) for var in vars if not var.is_external_input]
    info["designated_outputs"] = [
        (var.name, var.type) for var in vars if var.tags.get("designated_output", False) == "True"
    ]
    return info


@_track_function_call
def install_dependencies(user: str, dataflow: str, version: str = "latest") -> str:
    """Returns a string for the user to install dependencies.

    :param user:
    :param dataflow:
    :param version:
    :return:
    """
    local_file_path = USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow)
    if not os.path.exists(local_file_path):
        logger.info(
            "Dataflow does not exist locally. Can't provide details on how to install dependencies."
        )
        return ""
    return f"pip install -r {local_file_path}/requirements.txt"


from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version


@_track_function_call
def are_py_dependencies_satisfied(user, dataflow, version="latest", engine: str = "pip"):
    """Given a commit_ish, user & dataflow, reads the requirements.txt and checks if
    those dependencies have been satisfied in the currently running python interpreter.

    :param user:
    :param dataflow:
    :param version:
    :param engine: pip; conda not supported yet.
    :return:
    """
    if version == "latest":
        version = "main"
    requirements_path = (
        USER_PATH.format(commit_ish=version, user=user, dataflow=dataflow) + "/requirements.txt"
    )

    if not os.path.exists(requirements_path):
        logger.info(f"requirements.txt not found at {requirements_path}")
        return True

    # Get list of currently installed packages
    with open(requirements_path, "r") as req_file:
        lines = req_file.readlines()

        for line in lines:
            line = line.strip()
            equals = line.find("=")
            less_than = line.find("<")
            greater_than = line.find(">")
            version_marker = min(equals, less_than, greater_than)
            if version_marker > 0:
                package_name, required_version = line[:version_marker], line[version_marker + 1 :]
            else:
                package_name = line
                required_version = None
            required_version  # here for now...
            try:
                installed_version = pkg_version(package_name)
                installed_version  # here for now..
            except PackageNotFoundError:
                logger.info(f"Package '{package_name}' is not installed.")
                return False

            # TODO: Check version if specified
            # if required_version and installed_version != required_version:
            #     logger.info(
            #         f"Package '{package_name}' version mismatch. Required: {required_version}, Installed: {installed_version}")
            #     return False

    logger.info("All requirements are satisfied.")
    return True


@_track_function_call
def list_dataflows(commit_ish: str = "latest", user: str = None):
    """Lists dataflows locally downloaded based on commit_ish and user.

    :param commit_ish:
    :param user:
    :return:
    """
    if commit_ish == "latest":
        commit_ish = "main"
    if not os.path.exists(DATAFLOW_FOLDER):
        # TODO better error message here.
        logger.info(f"Folder {DATAFLOW_FOLDER} does not exist.")
        return []

    dataflows = []

    for ci in os.listdir(DATAFLOW_FOLDER):
        if (
            (commit_ish and ci != commit_ish)
            or os.path.isfile(os.path.join(DATAFLOW_FOLDER, ci))
            or ci.startswith(".")
        ):
            continue
        commit_path = os.path.join(DATAFLOW_FOLDER, ci, "contrib", "hamilton", "contrib", "user")
        for usr in os.listdir(commit_path):
            if (
                (user and usr != user)
                or os.path.isfile(os.path.join(commit_path, usr))
                or usr.startswith(".")
            ):
                continue
            user_path = os.path.join(commit_path, usr)
            for df in os.listdir(user_path):
                if os.path.isfile(os.path.join(user_path, df)) or df.startswith("."):
                    continue
                dataflows.append((ci, usr, df))

    # for ci, usr, df in dataflows:
    #     logger.info(f"version: {ci}, user: {usr}, dataflow: {df}")

    return dataflows


@_track_function_call
def find(query: str, commit_ish: str = None, user: str = None):
    """Searches for locally downloaded dataflows based on a query string.
    :param query:
    :param commit_ish:
    :param user:
    :return:
    """
    if not os.path.exists(DATAFLOW_FOLDER):
        logger.info(f"Folder {DATAFLOW_FOLDER} does not exist.")
        return []

    matches = set()

    for ci in os.listdir(DATAFLOW_FOLDER):
        if (
            (commit_ish and ci != commit_ish)
            or os.path.isfile(os.path.join(DATAFLOW_FOLDER, ci))
            or ci.startswith(".")
        ):
            continue
        commit_path = os.path.join(DATAFLOW_FOLDER, ci, "contrib", "hamilton", "contrib", "user")
        for usr in os.listdir(commit_path):
            if (
                (user and usr != user)
                or os.path.isfile(os.path.join(commit_path, usr))
                or usr.startswith(".")
            ):
                continue
            user_path = os.path.join(commit_path, usr)
            for df in os.listdir(user_path):
                for root, _, files in os.walk(user_path + "/" + df):
                    for file in files:
                        if not (
                            file.startswith(".") or file.endswith(".py") or file.endswith(".md")
                        ):
                            continue
                        file_path = os.path.join(root, file)
                        with open(file_path, "r") as f:
                            # dumb search -- just see if the string is in the file verbatim.
                            content = f.read()
                            if query in content:
                                matches.add((ci, usr, df))
    # TODO: incorporate tags
    return matches


@_track_function_call
def init():
    """Creates a template for someone to add a new flow for

    Don't want to bite off having to setup a fork,etc. This will
    just create a new directory with template files.
    """
    # TODO:
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    _user = "zilto"
    _version = "latest"  # or a git commit hash
    _dataflow = "text_summarization"
    _module = import_module(_dataflow, _user, _version, overwrite=True)

    from hamilton.contrib.user.zilto import text_summarization  # noqa:F401

    dr = driver.Driver({}, _module)
    # logger.info(dr.list_available_variables())
    dr.display_all_functions("./dag", {"format": "png", "view": False})

    logger.info("Listing dataflows ---")
    dfs = list_dataflows(commit_ish=None, user="zilto")
    logger.info(dfs)
    logger.info("Listing chunks ---")
    chunks = find("chunk")
    logger.info(chunks)
    logger.info("Determining python dependencies ---")
    are_py_dependencies_satisfied(_user, _dataflow, _version)
    import pprint

    logger.info("Inspect module output ---")
    logger.info(pprint.pformat(inspect_module(_module)))
    logger.info("Resolve dataflow commit output ---")
    _version = resolve_dataflow_commit(_dataflow, _user)
    logger.info(_version)
    logger.info("Install dependencies output ---")
    logger.info(install_dependencies(_user, _dataflow, _version))
    logger.info("Inspect output ---")
    logger.info(inspect(_user, _dataflow, _version))
