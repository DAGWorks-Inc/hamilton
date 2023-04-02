import dataclasses
from collections import Counter
from typing import Any, Dict, Tuple, Type

import pytest

from hamilton import ad_hoc_utils, base, driver, graph
from hamilton.function_modifiers import base as fm_base
from hamilton.function_modifiers import source, value
from hamilton.function_modifiers.adapters import LoadFromDecorator, load_from
from hamilton.io.data_loaders import DataLoader, LoadType
from hamilton.registry import ADAPTER_REGISTRY


def test_default_adapters_are_available():
    assert len(ADAPTER_REGISTRY) > 0


def test_default_adapters_are_registered_once():
    assert "json" in ADAPTER_REGISTRY
    count_unique = {
        key: Counter([value.__class__.__qualname__ for value in values])
        for key, values in ADAPTER_REGISTRY.items()
    }
    for key, value_ in count_unique.items():
        for impl, count in value_.items():
            assert count == 1, (
                f"Adapter registered multiple times for {impl}. This should not"
                f" happen, as items should just be registered once."
            )


@dataclasses.dataclass
class MockDataLoader(DataLoader):
    required_param: int
    required_param_2: int
    required_param_3: str
    default_param: int = 4
    default_param_2: int = 5
    default_param_3: str = "6"

    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return issubclass(type_, int)

    def load_data(self, type_: Type[int]) -> Tuple[int, Dict[str, Any]]:
        return ..., {"required_param": self.required_param, "default_param": self.default_param}

    @classmethod
    def name(cls) -> str:
        return "mock"


def test_load_from_decorator_resolve_kwargs():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        required_param=source("1"),
        required_param_2=value(2),
        required_param_3=value("3"),
        default_param=source("4"),
        default_param_2=value(5),
    )

    dependency_kwargs, literal_kwargs = decorator.resolve_kwargs()
    assert dependency_kwargs == {"required_param": "1", "default_param": "4"}
    assert literal_kwargs == {"required_param_2": 2, "required_param_3": "3", "default_param_2": 5}


def test_load_from_decorator_validate_succeeds():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        required_param=source("1"),
        required_param_2=value(2),
        required_param_3=value("3"),
    )

    def fn(injected_data: int) -> int:
        return injected_data

    decorator.validate(fn)


def test_load_from_decorator_validate_succeeds_with_inject():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        inject_="injected_data",
        required_param=source("1"),
        required_param_2=value(2),
        required_param_3=value("3"),
    )

    def fn(injected_data: int, dependent_data: int) -> int:
        return injected_data + dependent_data

    decorator.validate(fn)


def test_load_from_decorator_validate_fails_dont_know_which_param_to_inject():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        required_param=source("1"),
        required_param_2=value(2),
        required_param_3=value("3"),
    )

    def fn(injected_data: int, other_possible_injected_data: int) -> int:
        return injected_data + other_possible_injected_data

    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator.validate(fn)


def test_load_from_decorator_validate_fails_inject_not_in_fn():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        inject_="injected_data",
        required_param=source("1"),
        required_param_2=value(2),
        required_param_3=value("3"),
    )

    def fn(dependent_data: int) -> int:
        return dependent_data

    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator.validate(fn)


def test_load_from_decorator_validate_fails_inject_missing_param():
    decorator = LoadFromDecorator(
        [MockDataLoader],
        required_param=source("1"),
        required_param_2=value(2),
        # This is commented out cause it'll be missing
        # required_param_3=value("3"),
    )

    def fn(data: int) -> int:
        return data

    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator.validate(fn)


@dataclasses.dataclass
class StringDataLoader(DataLoader):
    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return issubclass(str, type_)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        return "foo", {"loader": "string_data_loader"}

    @classmethod
    def name(cls) -> str:
        return "dummy"


@dataclasses.dataclass
class IntDataLoader(DataLoader):
    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return issubclass(int, type_)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        return 1, {"loader": "int_data_loader"}

    @classmethod
    def name(cls) -> str:
        return "dummy"


@dataclasses.dataclass
class IntDataLoaderClass2(DataLoader):
    @classmethod
    def applies_to(cls, type_: Type[Type]) -> bool:
        return issubclass(int, type_)

    def load_data(self, type_: Type[LoadType]) -> Tuple[LoadType, Dict[str, Any]]:
        return 2, {"loader": "int_data_loader_class_2"}

    @classmethod
    def name(cls) -> str:
        return "dummy"


def test_validate_fails_incorrect_type():
    decorator = LoadFromDecorator(
        [StringDataLoader, IntDataLoader],
    )

    def fn_str_inject(injected_data: str) -> str:
        return injected_data

    def fn_int_inject(injected_data: int) -> int:
        return injected_data

    def fn_bool_inject(injected_data: bool) -> bool:
        return injected_data

    # This is valid as there is one parameter and its a type that the decorator supports
    decorator.validate(fn_str_inject)

    # This is valid as there is one parameter and its a type that the decorator supports
    decorator.validate(fn_int_inject)

    # This is invalid as there is one parameter and it is not a type that the decorator supports
    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator.validate(fn_bool_inject)


def test_validate_selects_correct_type():
    decorator = LoadFromDecorator(
        [StringDataLoader, IntDataLoader],
    )

    def fn_str_inject(injected_data: str) -> str:
        return injected_data

    def fn_int_inject(injected_data: int) -> int:
        return injected_data

    def fn_bool_inject(injected_data: bool) -> bool:
        return injected_data

    # This is valid as there is one parameter and its a type that the decorator supports
    decorator.validate(fn_str_inject)

    # This is valid as there is one parameter and its a type that the decorator supports
    decorator.validate(fn_int_inject)

    # This is invalid as there is one parameter and it is not a type that the decorator supports
    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator.validate(fn_bool_inject)


# Note that this tests an internal API, but we would like to test this to ensure
# class selection is correct
def test_load_from_resolves_correct_class():
    decorator = LoadFromDecorator(
        [StringDataLoader, IntDataLoader, IntDataLoaderClass2],
    )

    def fn_str_inject(injected_data: str) -> str:
        return injected_data

    def fn_int_inject(injected_data: int) -> int:
        return injected_data

    def fn_bool_inject(injected_data: bool) -> bool:
        return injected_data

    # This is valid as there is one parameter and its a type that the decorator supports
    assert decorator._resolve_loader_class(fn_str_inject) == StringDataLoader
    assert decorator._resolve_loader_class(fn_int_inject) == IntDataLoaderClass2
    # This is invalid as there is one parameter and it is not a type that the decorator supports
    with pytest.raises(fm_base.InvalidDecoratorException):
        decorator._resolve_loader_class(fn_bool_inject)


# End-to-end tests are probably cleanest
# We've done a bunch of tests of internal structures for other decorators,
# but that leaves the testing brittle
# We don't test the driver, we just use the function_graph to tests the nodes
def test_load_from_decorator_end_to_end():
    @LoadFromDecorator(
        [StringDataLoader, IntDataLoader, IntDataLoaderClass2],
    )
    def fn_str_inject(injected_data: str) -> str:
        return injected_data

    config = {}
    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    fg = graph.FunctionGraph(
        ad_hoc_utils.create_temporary_module(fn_str_inject), config=config, adapter=adapter
    )
    result = fg.execute(inputs={}, nodes=fg.nodes.values())
    assert result["fn_str_inject"] == "foo"
    assert result["load_data.fn_str_inject.injected_data"] == (
        "foo",
        {"loader": "string_data_loader"},
    )


@pytest.mark.parametrize(
    "source_",
    [
        value("tests/resources/data/test_load_from_data.json"),
        source("test_data"),
    ],
)
def test_load_from_decorator_json_file(source_):
    @load_from.json(path=source_)
    def raw_json_data(data: Dict[str, Any]) -> Dict[str, Any]:
        return data

    def number_employees(raw_json_data: Dict[str, Any]) -> int:
        return len(raw_json_data["employees"])

    def sum_age(raw_json_data: Dict[str, Any]) -> float:
        return sum([employee["age"] for employee in raw_json_data["employees"]])

    def mean_age(sum_age: float, number_employees: int) -> float:
        return sum_age / number_employees

    config = {}
    dr = driver.Driver(
        config,
        ad_hoc_utils.create_temporary_module(raw_json_data, number_employees, sum_age, mean_age),
        adapter=base.SimplePythonGraphAdapter(base.DictResult()),
    )
    result = dr.execute(
        ["mean_age"], inputs={"test_data": "tests/resources/data/test_load_from_data.json"}
    )
    assert result["mean_age"] - 32.33333 < 0.0001


def test_loader_fails_for_missing_attribute():
    with pytest.raises(AttributeError):
        load_from.not_a_loader(param=value("foo"))

