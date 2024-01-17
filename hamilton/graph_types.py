"""Module for external-facing graph constructs. These help the user navigate/manage the graph as needed."""
import typing
from dataclasses import dataclass

from hamilton import htypes, node

# This is a little ugly -- its just required for graph build, and works
# This indicates a larger smell though -- we need to have the right level of
# hierarchy to ensure we don't have to deal with this.
# The larger problem is that we have a few interfaces that are referred to by
# The core system (in defaults), and we have not managed to disentangle it yet.
if typing.TYPE_CHECKING:
    from hamilton import graph


@dataclass
class HamiltonNode:
    """External facing API for hamilton Nodes. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""

    name: str
    type: typing.Type
    tags: typing.Dict[str, typing.Union[str, typing.List[str]]]
    is_external_input: bool
    originating_functions: typing.Tuple[typing.Callable, ...]
    documentation: typing.Optional[str]
    required_dependencies: typing.Set[str]
    optional_dependencies: typing.Set[str]

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
                dep
                for dep, (type_, dep_type) in n.input_types.items()
                if dep_type == node.DependencyType.REQUIRED
            },
            optional_dependencies={
                dep
                for dep, (type_, dep_type) in n.input_types.items()
                if dep_type == node.DependencyType.OPTIONAL
            },
        )

    def __repr__(self):
        return f"{self.name}: {htypes.get_type_as_string(self.type)}"


@dataclass
class HamiltonGraph:
    """External facing API for Hamilton Graphs. Currently a list of nodes that
    allow you to trace forward/backwards in the graph. Will likely be adding some more capabilities:
        1. More metadata -- config + modules
        2. More utility functions -- make it easy to walk/do an action at each node
    For now, you have to implement walking on your own if you care about order.

    Note that you do not construct this class directly -- instead, you will get this at various points in the API.
    """

    nodes: typing.List[HamiltonNode]
    # store the original graph for internal use

    @staticmethod
    def from_graph(fn_graph: "graph.FunctionGraph") -> "HamiltonGraph":
        """Creates a HamiltonGraph from a FunctionGraph (Hamilton's internal representation).

        :param fn_graph: FunctionGraph to convert
        :return: HamiltonGraph created from the FunctionGraph
        """
        return HamiltonGraph(
            nodes=[HamiltonNode.from_node(n) for n in fn_graph.nodes.values()],
        )
