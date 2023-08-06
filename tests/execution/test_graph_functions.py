from typing import Callable, Dict, List, Union

import pytest

from hamilton import node
from hamilton.execution.graph_functions import nodes_between, topologically_sort_nodes


def _create_dummy_dag(
    adjacency_map: Dict[str, List[str]], dict_output: bool = False
) -> Union[List[node.Node], Dict[str, node.Node]]:
    name_map = {}
    for name, dependencies in adjacency_map.items():
        input_types = {dep: object for dep in dependencies}
        node_ = node.Node(
            name=name,
            typ=object,
            callabl=lambda **kwargs: object(),
            input_types=input_types,
        )
        name_map[name] = node_
    nodes = []
    for name, dependencies in adjacency_map.items():
        node_ = name_map[name]
        for dependency in dependencies:
            dep = name_map[dependency]
            dep.depended_on_by.append(node_)
            node_.dependencies.append(name_map[dependency])
        nodes.append(node_)
    if dict_output:
        return {node_.name: node_ for node_ in nodes}
    return nodes


def _assert_topologically_sorted(nodes, sorted_nodes):
    for node_ in nodes:
        for dep in node_.dependencies:
            assert sorted_nodes.index(dep.name) < sorted_nodes.index(node_.name)


@pytest.mark.parametrize(
    "dag_input, expected_sorted_nodes",
    [
        ({"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"], "e": ["d"]}, ["a", "b", "c", "d", "e"]),
        ({}, []),
        ({"a": []}, ["a"]),
        (
            {
                "a": ["b", "c"],
                "b": ["d", "e"],
                "c": ["d", "e"],
                "d": ["f"],
                "e": ["f", "g"],
                "f": ["h"],
                "g": ["h", "i"],
                "h": ["j", "k"],
                "i": ["k", "l"],
                "j": ["m"],
                "k": ["m", "n"],
                "l": ["n"],
                "m": ["o"],
                "n": ["o", "p"],
                "o": ["q"],
                "p": ["q"],
                "q": [],
            },
            ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"],
        ),
    ],
    ids=[
        "Simple DAG",
        "Empty DAG",
        "Single Node DAG",
        "Large DAG",
    ],
)
def test_topologically_sort_nodes(dag_input, expected_sorted_nodes):
    nodes = _create_dummy_dag(dag_input)
    sorted_nodes = [item.name for item in topologically_sort_nodes(nodes)]
    _assert_topologically_sorted(nodes, sorted_nodes)


def _is(name: str) -> Callable[[node.Node], bool]:
    def _inner(n: node.Node) -> bool:
        return n.name == name

    return _inner


@pytest.mark.parametrize(
    "dag_repr, expected_nodes_in_between, start_node, end_node",
    [
        (
            {"a": [], "b": ["a"], "c": ["b"]},
            {"b"},
            "a",
            "c",
        ),
        (
            {"a": [], "b": ["a"], "c": ["b"], "d": ["c"], "e": ["d"]},
            {"b", "c", "d"},
            "a",
            "e",
        ),
        (
            {
                "a": ["b", "c"],
                "b": ["d"],
                "c": ["d"],
                "d": ["e"],
                "e": ["f"],
                "f": ["g"],
                "g": ["h"],
                "h": ["i", "j"],
                "i": ["k"],
                "j": ["k"],
                "k": ["l"],
                "l": ["m", "n"],
                "m": ["o"],
                "n": ["o"],
                "o": ["p"],
                "p": ["q"],
                "q": [],
            },
            {"e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
            "q",
            "d",
        ),
        ({"a": [], "b": [], "c": ["a", "b"], "d": "c"}, {"c"}, "a", "d"),
    ],
    ids=["simple_base", "longer_chain", "complex_dag", "subdag_with_external_dep"],
)
def test_find_nodes_between(dag_repr, expected_nodes_in_between, start_node, end_node):
    nodes = _create_dummy_dag(dag_repr, dict_output=True)
    found_node, in_between = nodes_between(nodes[end_node], _is(start_node))
    assert found_node.name == start_node
    assert set(in_between) == {nodes[item] for item in expected_nodes_in_between}
