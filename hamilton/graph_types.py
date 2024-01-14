"""Module for external-facing graph constructs. These help the user navigate/manage the graph as needed."""
import typing
from dataclasses import dataclass, field

from hamilton import graph, htypes, node


@dataclass
class HamiltonNode:
    """External facing API for hamilton Nodes. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""

    name: str
    type: typing.Type
    tags: typing.Dict[str, str]
    is_external_input: bool
    originating_functions: typing.Tuple[typing.Callable, ...]
    documentation: typing.Optional[str]
    required_dependencies: typing.Set[str]
    optional_dependencies: typing.Set[str]
    # Store the original node for internal use
    _node: typing.Optional[node.Node] = field(default=None)

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
            _node=n,
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
    _function_graph: graph.FunctionGraph

    @staticmethod
    def from_graph(fn_graph: "graph.FunctionGraph") -> "HamiltonGraph":
        """Creates a HamiltonGraph from a FunctionGraph (Hamilton's internal representation).

        @param fn_graph: FunctionGraph to convert
        @return: HamiltonGraph created from the FunctionGraph
        """
        return HamiltonGraph(
            nodes=[HamiltonNode.from_node(n) for n in fn_graph.nodes.values()],
            _function_graph=fn_graph,
        )

    def get_execution_nodes(
        self,
        inputs: typing.Dict[str, typing.Any],
        final_vars: typing.List[str],
        overrides: typing.Dict[str, typing.Any],
        include_external_inputs: bool = False,
    ) -> typing.Collection[HamiltonNode]:
        """Gives the nodes involved in an execution of a DAG. Note that this is not an execution *plan*
        as we have no guarentees that (a) these are in topological order and (b) that these are in the order of
        execution. This is just the nodes that will be involved in the execution.

        :param inputs: Inputs, passed at runtime
        :param final_vars: Final variables, requested at runtime
        :param overrides: Overrides, passed at runtime
        :param include_external_inputs: We represents nodes that are expected to be passed in as nodes -- by default
            we don't include this, but we do have the option to.
        :return: A collection of HamiltonNodes that will be involved in the execution.
        """
        nodes_by_name = {n.name: n for n in self.nodes}
        all_nodes, input_vars = self._function_graph.get_upstream_nodes(
            final_vars, inputs, overrides
        )
        if not include_external_inputs:
            all_nodes -= input_vars
        return [nodes_by_name[n.name] for n in all_nodes]
