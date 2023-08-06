import collections
import random
from typing import Tuple

import pytest

import tests.resources.reuse_subdag
from hamilton import ad_hoc_utils, graph
from hamilton.function_modifiers import (
    InvalidDecoratorException,
    config,
    parameterized_subdag,
    recursive,
    subdag,
    value,
)
from hamilton.function_modifiers.base import NodeTransformer
from hamilton.function_modifiers.dependencies import source
from hamilton.function_modifiers.recursive import _validate_config_inputs


def test_collect_function_fns():
    val = random.randint(0, 100000)

    def test_fn(out: int = val) -> int:
        return out

    assert recursive.subdag.collect_functions(load_from=[test_fn])[0]() == test_fn()


def test_collect_functions_module():
    val = random.randint(0, 100000)

    def test_fn(out: int = val) -> int:
        return out

    assert (
        recursive.subdag.collect_functions(
            load_from=[ad_hoc_utils.create_temporary_module(test_fn)]
        )[0]()
        == test_fn()
    )


def test_assign_namespaces():
    assert recursive.assign_namespace(node_name="foo", namespace="bar") == "bar.foo"


def foo(a: int) -> int:
    return a


@config.when_not(some_config_param=True)
def bar(b: int) -> int:
    return b


@config.when(some_config_param=True)
def bar__alt() -> int:
    return 10


def test_subdag_validate_succeeds():
    def baz(foo: int, bar: str) -> str:
        return bar * foo

    decorator = recursive.subdag(
        foo,
        bar,
        inputs={},
        config={},
    )
    decorator.validate(baz)


def test_subdag_basic_no_parameterization():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(foo, bar, inputs={}, config={})
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    # subdags have prefixed names
    assert "baz.foo" in nodes
    assert "baz.bar" in nodes
    # but we expect our outputs to exist as well
    assert "baz" in nodes
    assert nodes["baz"].callable(**{"baz.foo": 10, "baz.bar": 20}) == 30


def test_subdag_assigns_non_final_tag():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(foo, bar, inputs={}, config={})
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    # subdags ensure that these nodes are non-final
    assert nodes["baz.foo"].tags[NodeTransformer.NON_FINAL_TAG] is True
    assert nodes["baz.foo"].tags[NodeTransformer.NON_FINAL_TAG] is True


def test_subdag_basic_simple_parameterization():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(
        foo,
        bar,
        config={},
        inputs={"a": value(1), "b": value(2)},
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    # This doesn't necessarily have to be part of the contract, but we're testing now to ensure that it works
    assert "baz.a" in nodes
    assert nodes["baz.a"]() == 1
    assert "baz.b" in nodes
    assert nodes["baz.b"]() == 2


def test_subdag_basic_source_parameterization():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(foo, bar, inputs={"a": source("c"), "b": source("d")}, config={})
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    # These aren't entirely part of the contract, but they're required for the
    # way we're currently implementing it. See https://github.com/dagworks-inc/hamilton/issues/201
    assert "baz.a" in nodes
    assert nodes["baz.a"](c=1) == 1
    assert "baz.b" in nodes
    assert nodes["baz.b"](d=2) == 2


def test_subdag_handles_config_assignment():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(
        foo,
        bar,
        bar__alt,
        inputs={"a": value(1)},
        config={"some_config_param": True},
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    assert nodes["baz.bar"]() == 10


def test_subdag_allows_config_as_input():
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    decorator = recursive.subdag(
        foo,
        bar,
        bar__alt,
        inputs={},
        config={"some_config_param": False, "b": 100},
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(baz, {})}
    assert nodes["baz.b"]() == 100
    assert nodes["baz.bar"](**{"baz.b": 100}) == 100


def test_subdag_empty_config():
    @config.when_not(some_config_param=False)
    def baz(foo: int, bar: int) -> int:
        return foo + bar

    def subdag(baz: int) -> int:
        return baz

    decorator = recursive.subdag(baz, inputs={})
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(subdag, {})}
    assert "subdag.baz" in nodes
    assert "subdag" in nodes


def test_reuse_subdag_end_to_end():
    fg = graph.FunctionGraph.from_modules(tests.resources.reuse_subdag, config={"op": "subtract"})
    prefixless_nodes = []
    prefixed_nodes = collections.defaultdict(list)
    for name, node in fg.nodes.items():
        name_split = name.split(".")
        if len(name_split) == 1:
            prefixless_nodes.append(node)
        else:
            namespace, name = name_split
            prefixed_nodes[namespace].append(node)
    node_set = set(fg.nodes)
    assert {
        "e_1",
        "e_2",
        "e_3",
        "e_4",
        "f_1",
        "f_2",
        "f_3",
        "f_4",
    } - node_set == set()  # All the nodes outputted by our subdags
    assert {
        "v1.d",
        "v2.d",
        "v3.d",
        "v4.d",
        "v1.e",
        "v2.e",
        "v3.e",
        "v4.e",
        "v1.f",
        "v2.f",
        "v3.f",
        "v4.f",
    } - node_set == set()  # All these nodes must be in the DAG -- they're all the namespaced nodes
    assert {
        "v1.e",
        "v2.e",
        "v3.e",
        "v4.e",
        "v1.f",
        "v2.f",
        "v3.f",
        "v4.f",
    } - node_set == set()  # All these nodes must be in the DAG
    assert {"a", "b"} - node_set == set()  # common nodes shared by the DAG, not as part of subdags
    assert {"e", "f", "d"} - node_set == {
        "e",
        "f",
        "d",
    }  # We've defined a node e, f, and d, but they're only namespaced in subdags
    assert {
        "v1.c",
        "v2.c",
        "v3.c",
    } - node_set == set()  # These are all static values that are namespaced in the subDAGs
    # The following are all static values
    assert fg.nodes["v1.c"].callable() == 10
    assert fg.nodes["v2.c"].callable() == 20
    assert fg.nodes["v3.c"].callable() == 30

    # Source assigned this
    assert list(fg.nodes["v4.e"].input_types)[0] == "v4.c"
    assert list(fg.nodes["v4.c"].input_types)[0] == "b"
    assert fg.nodes["v4.c"].callable(b=1234) == 1234
    # # Check that the config is assigned and overwritten correctly
    assert fg.nodes["v1.d"].callable(**{"v1.c": 10, "a": 100}) == 100 - 10
    assert fg.nodes["v3.d"].callable(**{"v3.c": 10, "a": 100}) == 100 + 10
    res = fg.execute(nodes=[fg.nodes["sum_everything"]])
    assert res["sum_everything"] == 318


def test_parameterized_subdag():
    def bar(input_1: int) -> int:
        return input_1 + 1

    def foo(input_2: int) -> int:
        return input_2 + 1

    @config.when(baz_version="standard")
    def baz__standard(foo: int, bar: int) -> int:
        return foo + bar

    @config.when(baz_version="alternate")
    def baz__alternate(foo: int, bar: int) -> int:
        return foo * bar

    def subdag_processor(foo: int, bar: int, baz: int) -> Tuple[int, int, int]:
        return foo, bar, baz

    fns = [bar, foo, baz__standard, baz__alternate]

    decorator = parameterized_subdag(
        *fns,
        inputs={"input_1": source("external_input_1"), "input_2": source("external_input_2")},
        config={"baz_version": "standard"},
        v0={},
        v1={"config": {"baz_version": "standard"}, "inputs": {"input_1": value(100)}},
        v2={
            "config": {"baz_version": "alternate"},
            "inputs": {"input_2": value(200)},
        },
    )
    dag_generated = decorator.generate_nodes(subdag_processor, {})
    assert len(dag_generated) == len(set([item.name for item in dag_generated]))
    nodes_by_name = {node.name: node for node in dag_generated}
    assert "v0" in nodes_by_name
    assert "v1" in nodes_by_name
    assert "v2" in nodes_by_name
    assert "v0.baz" in nodes_by_name
    assert "v1.baz" in nodes_by_name
    assert "v2.baz" in nodes_by_name
    assert nodes_by_name["v0.baz"].callable(**{"v0.foo": 1, "v0.bar": 2}) == 3
    assert nodes_by_name["v1.baz"].callable(**{"v1.foo": 1, "v1.bar": 2}) == 3
    assert nodes_by_name["v2.baz"].callable(**{"v2.foo": 1, "v2.bar": 2}) == 2


def test_nested_subdag():
    def bar(input_1: int) -> int:
        return input_1 + 1

    def foo(input_2: int) -> int:
        return input_2 + 1

    @subdag(
        foo,
        bar,
    )
    def inner_subdag(foo: int, bar: int) -> Tuple[int, int]:
        return foo, bar

    @subdag(inner_subdag, inputs={"input_2": value(10)}, config={"plus_one": True})
    def outer_subdag_1(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    @subdag(inner_subdag, inputs={"input_2": value(3)}, config={"plus_one": False})
    def outer_subdag_2(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    def sum_all(outer_subdag_1: int, outer_subdag_2: int) -> int:
        return outer_subdag_1 + outer_subdag_2

    # we only need to generate from the outer subdag
    # as it refers to the inner one
    full_module = ad_hoc_utils.create_temporary_module(outer_subdag_1, outer_subdag_2, sum_all)
    fg = graph.FunctionGraph.from_modules(full_module, config={})
    assert "outer_subdag_1" in fg.nodes
    assert "outer_subdag_2" in fg.nodes
    res = fg.execute(nodes=[fg.nodes["sum_all"]], inputs={"input_1": 2})
    # This is effectively the function graph
    assert res["sum_all"] == sum_all(
        outer_subdag_1(inner_subdag(bar(2), foo(10))), outer_subdag_2(inner_subdag(bar(2), foo(3)))
    )


def test_nested_subdag_with_config():
    def bar(input_1: int) -> int:
        return input_1 + 1

    @config.when(broken=False)
    def foo(input_2: int) -> int:
        return input_2 + 1

    @subdag(
        foo,
        bar,
    )
    def inner_subdag(foo: int, bar: int) -> Tuple[int, int]:
        return foo, bar

    @subdag(inner_subdag, inputs={"input_2": value(10)})
    def outer_subdag_1(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    @subdag(inner_subdag, inputs={"input_2": value(3)})
    def outer_subdag_2(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    def sum_all(outer_subdag_1: int, outer_subdag_2: int) -> int:
        return outer_subdag_1 + outer_subdag_2

    # we only need to generate from the outer subdag
    # as it refers to the inner one
    full_module = ad_hoc_utils.create_temporary_module(outer_subdag_1, outer_subdag_2, sum_all)
    fg = graph.FunctionGraph.from_modules(full_module, config={"broken": False})
    assert "outer_subdag_1" in fg.nodes
    assert "outer_subdag_2" in fg.nodes
    res = fg.execute(nodes=[fg.nodes["sum_all"]], inputs={"input_1": 2})
    # This is effectively the function graph
    assert res["sum_all"] == sum_all(
        outer_subdag_1(inner_subdag(bar(2), foo(10))), outer_subdag_2(inner_subdag(bar(2), foo(3)))
    )


def test_subdag_with_external_nodes_input():
    def bar(input_1: int) -> int:
        return input_1 + 1

    def foo(input_2: int) -> int:
        return input_2 + 1

    @subdag(foo, bar, external_inputs=["baz"])
    def foo_bar_baz(foo: int, bar: int, baz: int) -> int:
        return foo + bar + baz

    full_module = ad_hoc_utils.create_temporary_module(foo_bar_baz)
    fg = graph.FunctionGraph.from_modules(full_module, config={})
    # since we've provided it above,
    assert "baz" in fg.nodes
    assert fg.nodes["baz"].user_defined
    res = fg.execute(nodes=[fg.nodes["foo_bar_baz"]], inputs={"input_1": 2, "input_2": 3, "baz": 4})
    assert res["foo_bar_baz"] == foo_bar_baz(foo(3), bar(2), 4)


def test_parameterized_subdag_with_external_inputs_global():
    def bar(input_1: int) -> int:
        return input_1 + 1

    def foo(input_2: int) -> int:
        return input_2 + 1

    @parameterized_subdag(
        foo,
        bar,
        external_inputs=["baz"],
        foo_bar_baz_input_1={"inputs": {"input_1": value(10), "input_2": value(20)}},
        foo_bar_baz_input_2={"inputs": {"input_1": value(30), "input_2": value(40)}},
    )
    def foo_bar_baz(foo: int, bar: int, baz: int) -> int:
        return foo + bar + baz

    def foo_bar_baz_summed(foo_bar_baz_input_1: int, foo_bar_baz_input_2: int) -> int:
        return foo_bar_baz_input_1 + foo_bar_baz_input_2

    full_module = ad_hoc_utils.create_temporary_module(foo_bar_baz, foo_bar_baz_summed)
    fg = graph.FunctionGraph.from_modules(full_module, config={})
    # since we've provided it above,
    assert "baz" in fg.nodes
    assert fg.nodes["baz"].user_defined
    res = fg.execute(nodes=[fg.nodes["foo_bar_baz_summed"]], inputs={"baz": 100})
    assert res["foo_bar_baz_summed"] == foo_bar_baz(foo(10), foo(20), 100) + foo_bar_baz(
        bar(30), foo(40), 100
    )


def test_parameterized_subdag_with_config():
    def bar(input_1: int) -> int:
        return input_1 + 1

    @config.when(broken=False)
    def foo(input_2: int) -> int:
        return input_2 + 1

    @subdag(
        foo,
        bar,
    )
    def inner_subdag(foo: int, bar: int) -> Tuple[int, int]:
        return foo, bar

    @subdag(inner_subdag, inputs={"input_2": value(10)})
    def outer_subdag_1(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    @subdag(inner_subdag, inputs={"input_2": value(3)})
    def outer_subdag_2(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    def sum_all(outer_subdag_1: int, outer_subdag_2: int) -> int:
        return outer_subdag_1 + outer_subdag_2

    # we only need to generate from the outer subdag
    # as it refers to the inner one
    full_module = ad_hoc_utils.create_temporary_module(outer_subdag_1, outer_subdag_2, sum_all)
    fg = graph.FunctionGraph.from_modules(full_module, config={"broken": False})
    assert "outer_subdag_1" in fg.nodes
    assert "outer_subdag_2" in fg.nodes
    res = fg.execute(nodes=[fg.nodes["sum_all"]], inputs={"input_1": 2})
    # This is effectively the function graph
    assert res["sum_all"] == sum_all(
        outer_subdag_1(inner_subdag(bar(2), foo(10))), outer_subdag_2(inner_subdag(bar(2), foo(3)))
    )


def test_nested_parameterized_subdag_with_config():
    def bar(input_1: int) -> int:
        return input_1 + 1

    @config.when(broken=False)
    def foo(input_2: int) -> int:
        return input_2 + 1

    @parameterized_subdag(foo, bar, inner_subdag={})
    def inner_subdag(foo: int, bar: int) -> Tuple[int, int]:
        return foo, bar

    @parameterized_subdag(
        inner_subdag,
        outer_subdag_1={"inputs": {"input_2": value(10)}},
        outer_subdag_2={"inputs": {"input_2": value(3)}},
    )
    def outer_subdag_n(inner_subdag: Tuple[int, int]) -> int:
        return sum(inner_subdag)

    def sum_all(outer_subdag_1: int, outer_subdag_2: int) -> int:
        return outer_subdag_1 + outer_subdag_2

    # we only need to generate from the outer subdag
    # as it refers to the inner one
    full_module = ad_hoc_utils.create_temporary_module(outer_subdag_n, sum_all)
    fg = graph.FunctionGraph.from_modules(full_module, config={"broken": False})
    assert "outer_subdag_1" in fg.nodes
    assert "outer_subdag_2" in fg.nodes
    res = fg.execute(nodes=[fg.nodes["sum_all"]], inputs={"input_1": 2})
    # This is effectively the function graph
    assert res["sum_all"] == sum_all(
        outer_subdag_n(inner_subdag(bar(2), foo(10))), outer_subdag_n(inner_subdag(bar(2), foo(3)))
    )


@pytest.mark.parametrize(
    "config, inputs",
    [
        ({"foo": "bar"}, {}),
        ({"foo": "bar"}, {"input": source("foo")}),
        ({"foo": "bar"}, {"input": source("foo"), "input_2": value("foo")}),
    ],
)
def test_recursive_validate_config_inputs_happy(config, inputs):
    _validate_config_inputs(config, inputs)


@pytest.mark.parametrize(
    "config, inputs",
    [
        ({"foo": "bar"}, {"foo": source("baz")}),
        ({"foo": "bar"}, {"input": "baz"}),
    ],
)
def test_recursive_validate_config_inputs_sad(config, inputs):
    with pytest.raises(InvalidDecoratorException):
        _validate_config_inputs(config, inputs)
