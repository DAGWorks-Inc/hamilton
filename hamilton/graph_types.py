"""Module for external-facing graph constructs. These help the user navigate/manage the graph as needed."""
import typing
from dataclasses import dataclass, field

from hamilton import graph, node


@dataclass
class HamiltonNode:
    """External facing API for hamilton Nodes. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""

    name: str
    type: typing.Type
    tags: typing.Dict[str, str] = field(default_factory=dict)
    is_external_input: bool = field(default=False)
    originating_functions: typing.Optional[typing.Tuple[typing.Callable, ...]] = field(default=None)
    documentation: typing.Optional[str] = field(default=None)
    required_dependencies: typing.Set[str] = field(default_factory=list)
    optional_dependencies: typing.Set[str] = field(default_factory=list)

    @staticmethod
    def from_node(n: node.Node) -> "HamiltonNode":
        """Creates a HamiltonNode from a Node (Hamilton's internal representation).

        :param n: Node to create the Variable from.
        :return: HamiltonNode created from the Node.
        """
        return HamiltonNode(
            name=n.name,
            type=n.type,
            tags=n.tags,
            is_external_input=n.user_defined,
            originating_functions=n.originating_functions,
            documentation=n.documentation,
            required_dependencies={
                item.name
                for item in n.dependencies
                if not n.input_types[item.name][1] == node.DependencyType.REQUIRED
            },
            optional_dependencies={
                item.name
                for item in n.dependencies
                if n.input_types[item.name][1] == node.DependencyType.OPTIONAL
            },
        )


@dataclass
class HamiltonGraph:
    """External facing API for Hamilton Graphs. Currently a list of nodes that
    allow you to trace forward/backwards in the graph. Will likely be adding some more capabilities:
        1. More metadata -- config + modules
        2. More utility functions -- make it easy to walk/do an action at each node
    For now, you have to implement walking on your own if you care about order.
    """

    nodes: typing.List[HamiltonNode]

    @staticmethod
    def from_graph(fn_graph: "graph.FunctionGraph") -> "HamiltonGraph":
        """Creates a HamiltonGraph from a FunctionGraph (Hamilton's internal representation).

        @param fn_graph: FunctionGraph to convert
        @return: HamiltonGraph created from the FunctionGraph
        """
        return HamiltonGraph(nodes=[HamiltonNode.from_node(n) for n in fn_graph.nodes.values()])
