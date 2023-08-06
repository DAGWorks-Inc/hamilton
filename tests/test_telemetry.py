import configparser
import os
import sys
import uuid
from unittest import mock

import pytest

from hamilton import base, telemetry
from hamilton.experimental import h_async


@pytest.fixture
def blank_conf_file(tmp_path_factory):
    """Fixture to load config file without an ID"""
    file_location = tmp_path_factory.mktemp("home") / "hamilton.conf"
    with open(file_location, "w") as conf_file:
        conf = configparser.ConfigParser()
        conf.write(conf_file)
    return file_location


@pytest.fixture
def existing_conf_file(tmp_path_factory):
    """Fixture to load config file with an ID"""
    file_location = tmp_path_factory.mktemp("home") / "hamilton.conf"
    with open(file_location, "w") as conf_file:
        conf = configparser.ConfigParser()
        conf["DEFAULT"]["anonymous_id"] = "testing123-id"
        conf.write(conf_file)
    return file_location


def test__load_config_exists_with_id(existing_conf_file):
    """Tests loading a config that has an ID."""
    config = telemetry._load_config(existing_conf_file)
    a_id = config["DEFAULT"]["anonymous_id"]
    assert a_id == "testing123-id"


def test__load_config_exists_without_id(blank_conf_file):
    """Tests load from existing file without an ID."""
    config = telemetry._load_config(blank_conf_file)
    a_id = config["DEFAULT"]["anonymous_id"]
    assert str(uuid.UUID(a_id, version=4)) == a_id
    # check it was written back
    with open(blank_conf_file, "r") as conf_file:
        actual_config = configparser.ConfigParser()
        actual_config.read_file(conf_file)
    assert a_id == actual_config["DEFAULT"]["anonymous_id"]


def test__load_config_new(tmp_path_factory):
    """Tests no config file existing and one being created."""
    file_location = tmp_path_factory.mktemp("home") / "hamilton123.conf"
    config = telemetry._load_config(file_location)
    a_id = config["DEFAULT"]["anonymous_id"]
    assert str(uuid.UUID(a_id, version=4)) == a_id
    # check it was written back
    with open(file_location, "r") as conf_file:
        actual_config = configparser.ConfigParser()
        actual_config.read_file(conf_file)
    assert a_id == actual_config["DEFAULT"]["anonymous_id"]


def test__check_config_and_environ_for_telemetry_flag_not_present():
    """Tests not present in both."""
    conf = configparser.ConfigParser()
    actual = telemetry._check_config_and_environ_for_telemetry_flag(False, conf)
    assert actual is False


def test__check_config_and_environ_for_telemetry_flag_in_config():
    """tests getting from config."""
    conf = configparser.ConfigParser()
    conf["DEFAULT"]["telemetry_enabled"] = "tRuE"
    actual = telemetry._check_config_and_environ_for_telemetry_flag(False, conf)
    assert actual is True


@mock.patch.dict(os.environ, {"HAMILTON_TELEMETRY_ENABLED": "TrUe"})
def test__check_config_and_environ_for_telemetry_flag_in_env():
    """tests getting from env."""
    conf = configparser.ConfigParser()
    actual = telemetry._check_config_and_environ_for_telemetry_flag(False, conf)
    assert actual is True


@mock.patch.dict(os.environ, {"HAMILTON_TELEMETRY_ENABLED": "TrUe"})
def test__check_config_and_environ_for_telemetry_flag_env_overrides():
    """tests that env overrides the config."""
    conf = configparser.ConfigParser()
    conf["DEFAULT"]["telemetry_enabled"] = "FALSE"
    actual = telemetry._check_config_and_environ_for_telemetry_flag(False, conf)
    assert actual is True


@pytest.mark.skipif(
    os.environ.get("CI") != "true",
    reason="This test is currently flaky when run locally -- "
    "it has to be run exactly as it is in CI. "
    "As it is not a high-touch portion of the codebase, "
    "we default it not to run locally.",
)
def test_sanitize_error_general():
    """Tests sanitizing code in the general case.

    Run this test how circleci runs it.

    It's too hard to test code that isn't in the repo, or at least it hasn't occurred to
    me how to mock it easily.
    """
    try:
        # make a call in a hamilton module to mimic something from hamilton
        # but the stack trace should block the stack call from this function.
        telemetry.get_adapter_name(None)
    except AttributeError:
        actual = telemetry.sanitize_error(*sys.exc_info())
        # this strips the full path -- note: line changes in telemetry.py will change this...
        # so replace with line XXX
        import re

        actual = re.sub(r"line \d\d\d", "line XXX", actual)
        expected = (
            """...<USER_CODE>...\n...hamilton/telemetry.py, line XXX, in get_adapter_name\n"""
        )
        # if this fails -- run it how circleci runs it
        assert actual == expected


# classes for the tests below
class CustomAdapter(base.HamiltonGraphAdapter):
    def __init__(self, result_builder: base.ResultMixin):
        self.result_builder = result_builder


class CustomResultBuilder(base.ResultMixin):
    pass


@pytest.mark.parametrize(
    "adapter, expected",
    [
        (
            base.SimplePythonDataFrameGraphAdapter(),
            "hamilton.base.SimplePythonDataFrameGraphAdapter",
        ),
        (
            base.DefaultAdapter(),
            "hamilton.base.DefaultAdapter",
        ),
        (
            h_async.AsyncGraphAdapter(base.DictResult()),
            "hamilton.experimental.h_async.AsyncGraphAdapter",
        ),
        (CustomAdapter(base.DictResult()), "custom_adapter"),
    ],
)
def test_get_adapter_name(adapter, expected):
    """Tests get_adapter_name"""
    actual = telemetry.get_adapter_name(adapter)
    assert actual == expected


@pytest.mark.parametrize(
    "adapter, expected",
    [
        (base.SimplePythonDataFrameGraphAdapter(), "hamilton.base.PandasDataFrameResult"),
        (base.DefaultAdapter(), "hamilton.base.DictResult"),
        (
            base.SimplePythonGraphAdapter(base.NumpyMatrixResult()),
            "hamilton.base.NumpyMatrixResult",
        ),
        (
            base.SimplePythonGraphAdapter(base.StrictIndexTypePandasDataFrameResult()),
            "hamilton.base.StrictIndexTypePandasDataFrameResult",
        ),
        (base.SimplePythonGraphAdapter(CustomResultBuilder()), "custom_builder"),
        (h_async.AsyncGraphAdapter(base.DictResult()), "hamilton.base.DictResult"),
        (CustomAdapter(base.DictResult()), "hamilton.base.DictResult"),
        (CustomAdapter(CustomResultBuilder()), "custom_builder"),
    ],
)
def test_get_result_builder_name(adapter, expected):
    """Tests getting the result builder name"""
    actual = telemetry.get_result_builder_name(adapter)
    assert actual == expected


def test_is_telemetry_enabled_false():
    """Tests that we don't increment the counter when we're disabled."""
    before = telemetry.call_counter
    telemetry_enabled = telemetry.is_telemetry_enabled()
    assert telemetry.call_counter == before
    assert telemetry_enabled is False


@mock.patch("hamilton.telemetry.g_telemetry_enabled", True)
def test_is_telemetry_disabled_true():
    """Tests that we do increment the counter when we're enabled."""
    before = telemetry.call_counter
    telemetry_enabled = telemetry.is_telemetry_enabled()
    assert telemetry.call_counter == before + 1
    assert telemetry_enabled is True
