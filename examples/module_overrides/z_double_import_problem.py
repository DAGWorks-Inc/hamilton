import inspect

from hamilton import graph, node
from hamilton.node import NodeType

import tests.resources.dummy_functions
import tests.resources.dummy_functions_module_override


def foo() -> int:
    return 10


def bar(foo: int) -> int:
    return foo + 1


def create_testing_nodes():
    """Helper function for creating the nodes represented in dummy_functions.py."""
    nodes = {
        "A": node.Node(
            "A",
            inspect.signature(tests.resources.dummy_functions.A).return_annotation,
            "Function that should become part of the graph - A",
            tests.resources.dummy_functions.A,
            tags={"module": "tests.resources.dummy_functions"},
        ),
        "B": node.Node(
            "B",
            inspect.signature(tests.resources.dummy_functions.B).return_annotation,
            "Function that should become part of the graph - B",
            tests.resources.dummy_functions.B,
            tags={"module": "tests.resources.dummy_functions"},
        ),
        "C": node.Node(
            "C",
            inspect.signature(tests.resources.dummy_functions.C).return_annotation,
            "",
            tests.resources.dummy_functions.C,
            tags={"module": "tests.resources.dummy_functions"},
        ),
        "b": node.Node(
            "b",
            inspect.signature(tests.resources.dummy_functions.A).parameters["b"].annotation,
            node_source=NodeType.EXTERNAL,
        ),
        "c": node.Node(
            "c",
            inspect.signature(tests.resources.dummy_functions.A).parameters["c"].annotation,
            node_source=NodeType.EXTERNAL,
        ),
    }
    nodes["A"].dependencies.append(nodes["b"])
    nodes["A"].dependencies.append(nodes["c"])
    nodes["A"].depended_on_by.append(nodes["B"])
    nodes["A"].depended_on_by.append(nodes["C"])
    nodes["b"].depended_on_by.append(nodes["A"])
    nodes["c"].depended_on_by.append(nodes["A"])
    nodes["B"].dependencies.append(nodes["A"])
    nodes["C"].dependencies.append(nodes["A"])
    return nodes


def create_testing_nodes_override_B():
    """Helper function for creating the nodes represented in dummy_functions.py."""
    nodes = {
        "A": node.Node(
            "A",
            inspect.signature(tests.resources.dummy_functions.A).return_annotation,
            "Function that should become part of the graph - A",
            tests.resources.dummy_functions.A,
            tags={"module": "tests.resources.dummy_functions"},
        ),
        "B": node.Node(
            "B",
            inspect.signature(tests.resources.dummy_functions_module_override.B).return_annotation,
            "Function that should override function B.",
            tests.resources.dummy_functions_module_override.B,
            tags={"module": "tests.resources.dummy_functions_module_override"},
        ),
        "C": node.Node(
            "C",
            inspect.signature(tests.resources.dummy_functions.C).return_annotation,
            "",
            tests.resources.dummy_functions.C,
            tags={"module": "tests.resources.dummy_functions"},
        ),
        "b": node.Node(
            "b",
            inspect.signature(tests.resources.dummy_functions.A).parameters["b"].annotation,
            node_source=NodeType.EXTERNAL,
        ),
        "c": node.Node(
            "c",
            inspect.signature(tests.resources.dummy_functions.A).parameters["c"].annotation,
            node_source=NodeType.EXTERNAL,
        ),
    }
    nodes["A"].dependencies.append(nodes["b"])
    nodes["A"].dependencies.append(nodes["c"])
    nodes["A"].depended_on_by.append(nodes["B"])
    nodes["A"].depended_on_by.append(nodes["C"])
    nodes["b"].depended_on_by.append(nodes["A"])
    nodes["c"].depended_on_by.append(nodes["A"])
    nodes["B"].dependencies.append(nodes["A"])
    nodes["C"].dependencies.append(nodes["A"])
    return nodes


if __name__ == "__main__":
    # import __main__ as main
    # from hamilton import driver

    # dr = driver.Builder().with_modules(main)

    # # This produces an error
    # dr = dr.with_modules(main)

    # dr = dr.build()
    # print(dr.execute(inputs={}, final_vars=["bar"]))

    """Tests that we create a simple function graph."""
    expected = create_testing_nodes()
    actual = graph.create_function_graph(tests.resources.dummy_functions, config={})
    assert actual == expected

    # print(expected)
    # print(actual)

    override_expected = create_testing_nodes_override_B()
    print(override_expected)
    override_actual = graph.create_function_graph(
        tests.resources.dummy_functions,
        tests.resources.dummy_functions_module_override,
        config={},
        allow_module_overrides=True,
    )
    print(override_actual)
    assert override_expected == override_actual
