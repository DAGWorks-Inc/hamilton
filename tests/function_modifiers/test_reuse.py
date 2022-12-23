import collections
import random

import pytest

import tests.resources.reuse_subdag
from hamilton import ad_hoc_utils, graph
from hamilton.experimental.decorators import reuse
from hamilton.function_modifiers import base, config, value
from hamilton.function_modifiers.dependencies import source


def test_collect_function_fns():
    val = random.randint(0, 100000)

    def test_fn(out: int = val) -> int:
        return out

    assert reuse.reuse_functions.collect_functions(load_from=[test_fn])[0]() == test_fn()


def test_collect_functions_module():
    val = random.randint(0, 100000)

    def test_fn(out: int = val) -> int:
        return out

    assert (
        reuse.reuse_functions.collect_functions(
            load_from=[ad_hoc_utils.create_temporary_module(test_fn)]
        )[0]()
        == test_fn()
    )


def test_assign_namespaces():
    assert reuse.assign_namespace(node_name="foo", namespace="bar") == "bar.foo"


def foo(a: int) -> int:
    return a


@config.when_not(some_config_param=True)
def bar(b: int) -> int:
    return b


@config.when(some_config_param=True)
def bar__alt() -> int:
    return 10


def test_reuse_subdag_validate_outputs_succeeds():
    def test() -> reuse.MultiOutput(foo_result=int, bar_result=str):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    decorator.validate(test)


def test_reuse_subdag_validate_output_incorrect_type():
    def test() -> int:
        pass

    decorator = reuse.reuse_functions(
        with_inputs={},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    with pytest.raises(base.InvalidDecoratorException):
        decorator.validate(test)


def test_reuse_subdag_validate_output_fails_types_not_provided():
    def test() -> reuse.MultiOutput(foo_result=int):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    with pytest.raises(base.InvalidDecoratorException):
        decorator.validate(test)


def test_reuse_subdag_basic_no_parameterization():
    def test() -> reuse.MultiOutput(foo_result=int, bar_result=int):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(test, {})}
    # subdags have prefixed names
    assert "baz.foo" in nodes
    assert "baz.bar" in nodes
    # but we expect our outputs to exist as well
    assert "bar_result" in nodes
    assert "foo_result" in nodes


def test_reuse_subdag_basic_simple_parameterization():
    def test() -> reuse.MultiOutput(foo_result=int, bar_result=int):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={"a": value(1), "b": value(2)},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(test, {})}
    # This doesn't necessarily have to be part of the contract, but we're testing now to ensure that it works
    assert "baz.a" in nodes
    assert nodes["baz.a"]() == 1
    assert "baz.b" in nodes
    assert nodes["baz.b"]() == 2


def test_reuse_subdag_basic_source_parameterization():
    def test() -> reuse.MultiOutput(foo_result=int, bar_result=int):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={"a": source("c"), "b": source("d")},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={},
        load_from=[foo, bar],
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(test, {})}
    # These aren't entirely part of the contract, but they're required for the
    # way we're currently implementing it. See https://github.com/stitchfix/hamilton/issues/201
    assert "baz.a" in nodes
    assert nodes["baz.a"](c=1) == 1
    assert "baz.b" in nodes
    assert nodes["baz.b"](d=2) == 2


def test_reuse_subdag_handles_config_assignment():
    def test() -> reuse.MultiOutput(foo_result=int, bar_result=int):
        pass

    decorator = reuse.reuse_functions(
        with_inputs={"a": value(1)},
        namespace="baz",
        outputs={"foo": "foo_result", "bar": "bar_result"},
        with_config={"some_config_param": True},
        load_from=[foo, bar, bar__alt],
    )
    nodes = {node_.name: node_ for node_ in decorator.generate_nodes(test, {})}
    assert nodes["baz.bar"]() == 10


def test_reuse_subdag_end_to_end():
    fg = graph.FunctionGraph(tests.resources.reuse_subdag, config={"op": "subtract"})
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
