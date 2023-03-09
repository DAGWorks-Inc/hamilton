import os
from typing import Callable
from unittest import mock

import pytest

from hamilton.function_modifiers import ResolveAt, base, extract_columns, resolve


@pytest.fixture(autouse=True)
def power_user_mode():
    with mock.patch.dict(os.environ, {"HAMILTON_POWER_USER_MODE": "true"}):
        yield


@pytest.mark.parametrize(
    "fn,required,optional",
    [
        (lambda: 1, [], []),
        (lambda a, b: 1, ["a", "b"], []),
        (lambda a, b=1: 1, ["a"], ["b"]),
        (lambda a=1, b=1: 1, [], ["a", "b"]),
    ],
)
def test_extract_and_validate_params_happy(fn: Callable, required: Callable, optional: Callable):
    from hamilton.function_modifiers import delayed

    assert delayed.extract_and_validate_params(fn) == (required, optional)


@pytest.mark.parametrize(
    "fn",
    [
        lambda **kwargs: 1,
        lambda a, b, *args: 1,
        lambda a, b, *args, **kwargs: 1,
    ],
)
def test_extract_and_validate_params_unhappy(fn: Callable):
    from hamilton.function_modifiers import delayed

    with pytest.raises(base.InvalidDecoratorException):
        delayed.extract_and_validate_params(fn)


def test_dynamic_resolves():
    decorator = resolve(
        when=ResolveAt.CONFIG_AVAILABLE,
        decorate_with=lambda cols_to_extract: extract_columns(*cols_to_extract),
    )
    decorator_resolved = decorator.resolve(
        {"cols_to_extract": ["a", "b"]}, fn=test_dynamic_resolves
    )
    # This uses an internal component of extract_columns
    # We may want to add a little more comprehensive testing
    # But for now this will work
    assert decorator_resolved.columns == ("a", "b")


def test_dynamic_fails_without_config_provided():
    decorator = resolve(
        when=ResolveAt.CONFIG_AVAILABLE,
        decorate_with=lambda cols_to_extract: extract_columns(*cols_to_extract),
    )
    with pytest.raises(base.InvalidDecoratorException):
        decorator_resolved = decorator.resolve({}, fn=test_dynamic_fails_without_config_provided)
        # This uses an internal component of extract_columns
        # We may want to add a little more comprehensive testing
        # But for now this will work
        assert decorator_resolved.columns == ("a", "b")


def test_config_derivation():
    decorator = resolve(
        when=ResolveAt.CONFIG_AVAILABLE,
        decorate_with=lambda cols_to_extract, some_cols_you_might_want_to_extract=[]: extract_columns(
            *cols_to_extract + some_cols_you_might_want_to_extract
        ),
    )
    assert decorator.required_config() == ["cols_to_extract"]
    assert decorator.optional_config() == ["some_cols_you_might_want_to_extract"]


def test_delayed_with_optional():
    decorator = resolve(
        when=ResolveAt.CONFIG_AVAILABLE,
        decorate_with=lambda cols_to_extract, some_cols_you_might_want_to_extract=[
            "c"
        ]: extract_columns(*cols_to_extract + some_cols_you_might_want_to_extract),
    )
    resolved = decorator.resolve({"cols_to_extract": ["a", "b"]}, fn=test_delayed_with_optional)
    assert list(resolved.columns) == ["a", "b", "c"]
    resolved = decorator.resolve(
        {"cols_to_extract": ["a", "b"], "some_cols_you_might_want_to_extract": ["d"]},
        fn=test_delayed_with_optional,
    )
    assert list(resolved.columns) == ["a", "b", "d"]
