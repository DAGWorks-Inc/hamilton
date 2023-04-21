import pytest

from hamilton import function_modifiers
from hamilton.function_modifiers import base


def test_sanitize_function_name():
    assert base.sanitize_function_name("fn_name__v2") == "fn_name"
    assert base.sanitize_function_name("fn_name") == "fn_name"


def test_config_modifier_validate():
    def valid_fn() -> int:
        pass

    def valid_fn__this_is_also_valid() -> int:
        pass

    function_modifiers.config.when(key="value").validate(valid_fn__this_is_also_valid)
    function_modifiers.config.when(key="value").validate(valid_fn)

    def invalid_function__() -> int:
        pass

    with pytest.raises(base.InvalidDecoratorException):
        function_modifiers.config.when(key="value").validate(invalid_function__)


def test_config_when():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key="value")
    assert annotation.resolve(config_when_fn, {"key": "value"}) is not None
    assert annotation.resolve(config_when_fn, {"key": "wrong_value"}) is None


def test_config_when_not():
    def config_when_not_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not(key="value")
    assert annotation.resolve(config_when_not_fn, {"key": "other_value"}) is not None
    assert annotation.resolve(config_when_not_fn, {"key": "value"}) is None


def test_config_when_in():
    def config_when_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_in(key=["valid_value", "another_valid_value"])
    assert annotation.resolve(config_when_in_fn, {"key": "valid_value"}) is not None
    assert annotation.resolve(config_when_in_fn, {"key": "another_valid_value"}) is not None
    assert annotation.resolve(config_when_in_fn, {"key": "not_a_valid_value"}) is None


def test_config_when_not_in():
    def config_when_not_in_fn() -> int:
        pass

    annotation = function_modifiers.config.when_not_in(
        key=["invalid_value", "another_invalid_value"]
    )
    assert annotation.resolve(config_when_not_in_fn, {"key": "invalid_value"}) is None
    assert annotation.resolve(config_when_not_in_fn, {"key": "another_invalid_value"}) is None
    assert annotation.resolve(config_when_not_in_fn, {"key": "valid_value"}) is not None


def test_config_name_resolution():
    def fn__v2() -> int:
        pass

    annotation = function_modifiers.config.when(key="value")
    assert annotation.resolve(fn__v2, {"key": "value"}).__name__ == "fn"


def test_config_when_with_custom_name():
    def config_when_fn() -> int:
        pass

    annotation = function_modifiers.config.when(key="value", name="new_function_name")
    assert annotation.resolve(config_when_fn, {"key": "value"}).__name__ == "new_function_name"


def test_config_base_resolve_nodes():
    def config_fn() -> int:
        pass

    annotation = function_modifiers.config(lambda conf: conf["key"] == "value")
    assert annotation.resolve(config_fn, {"key": "value"}) is not None


def test_config_base_resolve_nodes_no_resolve():
    def config_fn() -> int:
        pass

    annotation = function_modifiers.config(lambda conf: conf.get("key") == "value")
    assert annotation.resolve(config_fn, {}) is None


def test_config_base_resolve_nodes_end_to_end():
    def config_fn() -> int:
        pass

    # tests that the full resolver works
    annotation = function_modifiers.config(lambda conf: conf.get("key") == "value")
    config_fn = annotation(config_fn)
    nodes = base.resolve_nodes(config_fn, {})
    assert len(nodes) == 0
