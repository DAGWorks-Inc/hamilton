"""
This module should not have any real business logic.

It should only house the graph & things required to create and traverse one.

Note: one should largely consider the code in this module to be "private".
"""
import logging
from enum import Enum
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Tuple, Type

from hamilton import base, node
from hamilton.execution import graph_functions
from hamilton.execution.graph_functions import combine_config_and_inputs, execute_subdag
from hamilton.function_modifiers import base as fm_base
from hamilton.graph_utils import find_functions
from hamilton.htypes import types_match
from hamilton.node import Node

logger = logging.getLogger(__name__)

PATH_COLOR = "red"


class VisualizationNodeModifiers(Enum):
    """Enum of all possible node modifiers for visualization."""

    IS_OUTPUT = 1
    IS_PATH = 2
    IS_USER_INPUT = 3
    IS_OVERRIDE = 4


def add_dependency(
    func_node: node.Node,
    func_name: str,
    nodes: Dict[str, node.Node],
    param_name: str,
    param_type: Type,
    adapter: base.HamiltonGraphAdapter,
):
    """Adds dependencies to the node objects.

    This will add user defined inputs to the dictionary of nodes in the graph.

    :param func_node: the node we're pulling dependencies from.
    :param func_name: the name of the function we're inspecting.
    :param nodes: nodes representing the graph. This function mutates this object and underlying objects.
    :param param_name: the parameter name we're looking for/adding as a dependency.
    :param param_type: the type of the parameter.
    :param adapter: The adapter that adapts our node type checking based on the context.
    """
    if param_name in nodes:
        # validate types match
        required_node = nodes[param_name]
        if not types_match(adapter, param_type, required_node.type):
            raise ValueError(
                f"Error: {func_name} is expecting {param_name}:{param_type}, but found "
                f"{param_name}:{required_node.type}. \nHamilton does not consider these types to be "
                f"equivalent. If you believe they are equivalent, please reach out to the developers."
                f"Note that, if you have types that are equivalent for your purposes, you can create a "
                f"graph adapter that checks the types against each other in a more lenient manner."
            )
    else:
        # this is a user defined var
        required_node = node.Node(param_name, param_type, node_source=node.NodeType.EXTERNAL)
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def update_dependencies(
    nodes: Dict[str, node.Node], adapter: base.HamiltonGraphAdapter, reset_dependencies: bool = True
):
    """Adds dependencies to a dictionary of nodes. If in_place is False,
    it will deepcopy the dict + nodes and return that. Otherwise it will
    mutate + return the passed-in dict + nodes.

    :param in_place: Whether or not to modify in-place, or copy/return
    :param nodes: Nodes that form the DAG we're updating
    :param adapter: Adapter to use for type checking
    :param reset_dependencies: Whether or not to reset the dependencies. If they are not set this is
    unnecessary, and we can save yet another pass. Note that `reset` will perform an in-place
    operation.
    :return: The updated nodes
    """
    # copy without the dependencies to avoid duplicates
    if reset_dependencies:
        nodes = {k: v.copy(include_refs=False) for k, v in nodes.items()}
    for node_name, n in list(nodes.items()):
        for param_name, (param_type, _) in n.input_types.items():
            add_dependency(n, node_name, nodes, param_name, param_type, adapter)
    return nodes


def create_function_graph(
    *modules: ModuleType,
    config: Dict[str, Any],
    adapter: base.HamiltonGraphAdapter,
    fg: Optional["FunctionGraph"] = None,
) -> Dict[str, node.Node]:
    """Creates a graph of all available functions & their dependencies.
    :param modules: A set of modules over which one wants to compute the function graph
    :param config: Dictionary that we will inspect to get values from in building the function graph.
    :param adapter: The adapter that adapts our node type checking based on the context.
    :return: list of nodes in the graph.
    If it needs to be more complicated, we'll return an actual networkx graph and get all the rest of the logic for free
    """
    if fg is None:
        nodes = {}  # name -> Node
    else:
        nodes = fg.nodes
    functions = sum([find_functions(module) for module in modules], [])

    # create nodes -- easier to just create this in one loop
    for func_name, f in functions:
        for n in fm_base.resolve_nodes(f, config):
            if n.name in config:
                continue  # This makes sure we overwrite things if they're in the config...
            if n.name in nodes:
                raise ValueError(
                    f"Cannot define function {n.name} more than once."
                    f" Already defined by function {f}"
                )
            nodes[n.name] = n
    # add dependencies -- now that all nodes exist, we just run through edges & validate graph.
    nodes = update_dependencies(nodes, adapter, reset_dependencies=False)  # no dependencies
    # present yet
    for key in config.keys():
        if key not in nodes:
            nodes[key] = node.Node(key, Any, node_source=node.NodeType.EXTERNAL)
    return nodes


def create_graphviz_graph(
    nodes: Set[node.Node],
    comment: str,
    graphviz_kwargs: dict,
    node_modifiers: Dict[str, Set[VisualizationNodeModifiers]],
    strictly_display_only_nodes_passed_in: bool,
) -> "graphviz.Digraph":  # noqa: F821
    """Helper function to create a graphviz graph.

    :param nodes: The set of computational nodes
    :param comment: The comment to have on the graph.
    :param graphviz_kwargs: kwargs to pass to create the graph.
        e.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
    :param node_modifiers: A dictionary of node names to dictionaries of node attributes to modify.
    :param strictly_display_only_nodes_passed_in: If True, only display the nodes passed in. Else defaults to displaying
        also what nodes a node depends on (i.e. all nodes that feed into it).
    :return: a graphviz.Digraph; use this to render/save a graph representation.
    """
    import graphviz

    digraph = graphviz.Digraph(comment=comment, **graphviz_kwargs)
    for n in nodes:
        label = n.name
        other_args = {}
        # checks if the node has any modifiers
        if n.name in node_modifiers:
            modifiers = node_modifiers[n.name]
            # if node is an output, then modify the node to be a rectangle
            if VisualizationNodeModifiers.IS_OUTPUT in modifiers:
                other_args["shape"] = "rectangle"
            if VisualizationNodeModifiers.IS_PATH in modifiers:
                other_args["color"] = PATH_COLOR

            if VisualizationNodeModifiers.IS_USER_INPUT in modifiers:
                other_args["style"] = "dashed"
                label = f"Input: {n.name}"

            if VisualizationNodeModifiers.IS_OVERRIDE in modifiers:
                other_args["style"] = "dashed"
                label = f"Override: {n.name}"
        is_expand_node = n.node_role == node.NodeType.EXPAND
        is_collect_node = n.node_role == node.NodeType.COLLECT

        if is_collect_node or is_expand_node:
            other_args["peripheries"] = "2"
        digraph.node(n.name, label=label, **other_args)

    for n in list(nodes):
        for d in n.dependencies:
            if strictly_display_only_nodes_passed_in and d not in nodes:
                continue
            if (
                d not in nodes
                and d.name in node_modifiers
                and VisualizationNodeModifiers.IS_USER_INPUT in node_modifiers[d.name]
            ):
                digraph.node(d.name, label=f"Input: {d.name}", style="dashed")
            from_modifiers = node_modifiers.get(d.name, set())
            to_modifiers = node_modifiers.get(n.name, set())
            other_args = {}
            if (
                VisualizationNodeModifiers.IS_PATH in from_modifiers
                and VisualizationNodeModifiers.IS_PATH in to_modifiers
            ):
                other_args["color"] = PATH_COLOR
            is_collect_edge = n.node_role == node.NodeType.COLLECT
            is_expand_edge = d.node_role == node.NodeType.EXPAND

            if is_collect_edge:
                other_args["dir"] = "both"
                other_args["arrowtail"] = "crow"

            if is_expand_edge:
                other_args["dir"] = "both"
                other_args["arrowhead"] = "crow"
                other_args["arrowtail"] = "none"
            digraph.edge(d.name, n.name, **other_args)
    return digraph


def create_networkx_graph(
    nodes: Set[node.Node], user_nodes: Set[node.Node], name: str
) -> "networkx.DiGraph":  # noqa: F821
    """Helper function to create a networkx graph.

    :param nodes: The set of computational nodes
    :param user_nodes: The set of nodes that the user is providing inputs for.
    :param name: The name to have on the graph.
    :return: a graphviz.Digraph; use this to render/save a graph representation.
    """
    import networkx

    digraph = networkx.DiGraph(name=name)
    for n in nodes:
        digraph.add_node(n.name, label=n.name)
    for n in user_nodes:
        digraph.add_node(n.name, label=f"UD: {n.name}")

    for n in list(nodes) + list(user_nodes):
        for d in n.dependencies:
            digraph.add_edge(d.name, n.name)
    return digraph


class FunctionGraph(object):
    """Note: this object should be considered private until stated otherwise.

    That is, you should not try to build off of it directly without chatting to us first.
    """

    def __init__(
        self,
        nodes: Dict[str, Node],
        config: Dict[str, Any],
        adapter: base.HamiltonGraphAdapter = None,
    ):
        """Initializes a function graph from specified nodes. See note on `from_modules` if you
        start getting an error here because you use an internal API.

        :param nodes: Nodes, taken from the output of create_function_graph.
        :param config: this is configuration and/or initial data.
        :param adapter: adapts function building and graph execution for different contexts.
        """
        if adapter is None:
            adapter = base.SimplePythonDataFrameGraphAdapter()

        self._config = config
        self.nodes = nodes
        self.adapter = adapter

    @staticmethod
    def from_modules(
        *modules: ModuleType, config: Dict[str, Any], adapter: base.HamiltonGraphAdapter = None
    ):
        """Initializes a function graph from the specified modules. Note that this was the old
        way we constructed FunctionGraph -- this is not a public-facing API, so we replaced it
        with a constructor that takes in nodes directly. If you hacked in something using
        `FunctionGraph`, then you should be able to replace it with `FunctionGraph.from_modules`
        and it will work.

        :param modules: Modules to crawl, resolve to nodes
        :param config: Config to use for node resolution
        :param adapter: Adapter to use for node resolution, edge creation
        :return: a function graph.
        """

        nodes = create_function_graph(*modules, config=config, adapter=adapter)
        return FunctionGraph(nodes, config, adapter)

    def with_nodes(self, nodes: Dict[str, Node]) -> "FunctionGraph":
        """Creates a new function graph with the additional specified nodes.
        Note that if there is a duplication in the node definitions,
        it will error out.

        :param nodes: Nodes to add to the FunctionGraph
        :return: a new function graph.
        """
        # first check if there are any duplicates
        duplicates = set(self.nodes.keys()).intersection(set(nodes.keys()))
        if duplicates:
            raise ValueError(f"Duplicate node names found: {duplicates}")
        new_node_dict = update_dependencies({**self.nodes, **nodes}, self.adapter)
        return FunctionGraph(new_node_dict, self.config, self.adapter)

    @property
    def config(self):
        return self._config

    @property
    def decorator_counter(self) -> Dict[str, int]:
        return fm_base.DECORATOR_COUNTER

    def get_nodes(self) -> List[node.Node]:
        return list(self.nodes.values())

    def display_all(
        self,
        output_file_path: str = "test-output/graph-all.gv",
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
    ):
        """Displays & saves a dot file of the entire DAG structure constructed.

        :param output_file_path: the place to save the files.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: kwargs to be passed to the graphviz graph object to configure it.
            e.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        """
        all_nodes = set()
        node_modifiers = {}
        for n in self.nodes.values():
            if n.user_defined:
                node_modifiers[n.name] = {VisualizationNodeModifiers.IS_USER_INPUT}
            all_nodes.add(n)
        if render_kwargs is None:
            render_kwargs = {}
        if graphviz_kwargs is None:
            graphviz_kwargs = {}
        return self.display(
            all_nodes,
            output_file_path=output_file_path,
            render_kwargs=render_kwargs,
            graphviz_kwargs=graphviz_kwargs,
            node_modifiers=node_modifiers,
        )

    def has_cycles(self, nodes: Set[node.Node], user_nodes: Set[node.Node]) -> bool:
        """Checks that the graph created does not contain cycles.

        :param nodes: the set of nodes that need to be computed.
        :param user_nodes: the set of inputs that the user provided.
        :return: bool. True if cycles detected. False if not.
        """
        cycles = self.get_cycles(nodes, user_nodes)
        return True if cycles else False

    def get_cycles(self, nodes: Set[node.Node], user_nodes: Set[node.Node]) -> List[List[str]]:
        """Returns cycles found in the graph.

        :param nodes: the set of nodes that need to be computed.
        :param user_nodes: the set of inputs that the user provided.
        :return: list of cycles, which is a list of node names.
        """
        try:
            import networkx
        except ModuleNotFoundError:
            logger.exception(
                " networkx is required for detecting cycles in the function graph. Install it with:"
                '\n\n  pip install "sf-hamilton[visualization]" or pip install networkx \n\n'
            )
            return []
        digraph = create_networkx_graph(nodes, user_nodes, "Dependency Graph")
        cycles = list(networkx.simple_cycles(digraph))
        return cycles

    @staticmethod
    def display(
        nodes: Set[node.Node],
        output_file_path: Optional[str] = "test-output/graph.gv",
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
        node_modifiers: Dict[str, Set[VisualizationNodeModifiers]] = None,
        strictly_display_only_passed_in_nodes: bool = False,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Function to display the graph represented by the passed in nodes.

        :param nodes: the set of nodes that need to be computed.
        :param output_file_path: the path where we want to store the `dot` file + pdf picture. Pass in None to not save.
        :param render_kwargs: kwargs to be passed to the render function to visualize.
        :param graphviz_kwargs: kwargs to be passed to the graphviz graph object to configure it.
            e.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :param node_modifiers: a dictionary of node names to a dictionary of attributes to modify.
            e.g. {'node_name': {NodeModifiers.IS_USER_INPUT}} will set the node named 'node_name' to be a user input.
        :param strictly_display_only_passed_in_nodes: if True, only display the nodes passed in.  Else defaults to
            displaying also what nodes a node depends on (i.e. all nodes that feed into it).
        :return: the graphviz graph object if it was created. None if not.
        """
        # Check to see if optional dependencies have been installed.
        try:
            import graphviz  # noqa: F401
        except ModuleNotFoundError:
            logger.exception(
                " graphviz is required for visualizing the function graph. Install it with:"
                '\n\n  pip install "sf-hamilton[visualization]" or pip install graphviz \n\n'
            )
            return
        if graphviz_kwargs is None:
            graphviz_kwargs = {}
        if node_modifiers is None:
            node_modifiers = {}
        dot = create_graphviz_graph(
            nodes,
            "Dependency Graph",
            graphviz_kwargs,
            node_modifiers,
            strictly_display_only_passed_in_nodes,
        )
        kwargs = {"view": True}
        if render_kwargs and isinstance(render_kwargs, dict):
            kwargs.update(render_kwargs)
        if output_file_path:
            dot.render(output_file_path, **kwargs)
        return dot

    def get_impacted_nodes(self, var_changes: List[str]) -> Set[node.Node]:
        """DEPRECATED - use `get_downstream_nodes` instead."""
        logger.warning(
            "FunctionGraph.get_impacted_nodes is deprecated. "
            "Use `get_downstream_nodes` instead. This function will be removed"
            "in a future release."
        )
        return self.get_downstream_nodes(var_changes)

    def get_downstream_nodes(self, var_changes: List[str]) -> Set[node.Node]:
        """Given our function graph, and a list of nodes that are changed,
        returns the subgraph that they will impact.

        :param var_changes: the list of nodes that will change.
        :return: A set of all changed nodes.
        """
        nodes, user_nodes = self.directional_dfs_traverse(
            lambda n: n.depended_on_by, starting_nodes=var_changes
        )
        return nodes

    def get_upstream_nodes(
        self,
        final_vars: List[str],
        runtime_inputs: Dict[str, Any] = None,
        runtime_overrides: Dict[str, Any] = None,
    ) -> Tuple[Set[node.Node], Set[node.Node]]:
        """Given our function graph, and a list of desired output variables, returns the subgraph
        required to compute them.

        :param final_vars: the list of node names we want.
        :param runtime_inputs: runtime inputs to the DAG -- if not provided we will assume we're running at compile-time. Everything
        would then be required (even though it might be marked as optional), as we want to crawl
        the whole DAG. If we're at runtime, we want to just use the nodes that are provided,
        and not count the optional ones that are not.
        :param runtime_overrides: runtime overrides to the DAG -- if not provided we will assume
        we're running at compile-time.
        :return: a tuple of sets: - set of all nodes. - subset of nodes that human input is required for.
        """

        def next_nodes_function(n: node.Node) -> List[node.Node]:
            deps = []
            if runtime_overrides is not None and n.name in runtime_overrides:
                return deps
            if runtime_inputs is None:
                return n.dependencies
            for dep in n.dependencies:
                # If inputs is None, we want to assume its required, as it is a compile-time
                # dependency
                if (
                    dep.user_defined
                    and dep.name not in runtime_inputs
                    and dep.name not in self.config
                ):
                    _, dependency_type = n.input_types[dep.name]
                    if dependency_type == node.DependencyType.OPTIONAL:
                        continue
                deps.append(dep)
            return deps

        return self.directional_dfs_traverse(next_nodes_function, starting_nodes=final_vars)

    def nodes_between(self, start: str, end: str) -> Set[node.Node]:
        """Given our function graph, and a list of desired output variables, returns the subgraph
        required to compute them. Note that this returns an empty set if no path exists.


        :param start: the start of the subDAG we're looking for (inputs to it)
        :param end: the end of the subDAG we're looking for
        :return: The set of all nodes between start and end inclusive
        """

        end_node = self.nodes[end]
        start_node, between = graph_functions.nodes_between(
            self.nodes[end], lambda node_: node_.name == start
        )
        path_exists = start_node is not None
        if not path_exists:
            return set()
        return set(([start_node] if start_node is not None else []) + between + [end_node])

    def directional_dfs_traverse(
        self, next_nodes_fn: Callable[[node.Node], Collection[node.Node]], starting_nodes: List[str]
    ):
        """Traverses the DAG directionally using a DFS.

        :param next_nodes_fn: Function to give the next set of nodes
        :param starting_nodes: Which nodes to start at.
        :return: a tuple of sets:
            - set of all nodes.
            - subset of nodes that human input is required for.
        """
        nodes = set()
        user_nodes = set()

        def dfs_traverse(node: node.Node):
            nodes.add(node)
            for n in next_nodes_fn(node):
                if n not in nodes:
                    dfs_traverse(n)
            if node.user_defined:
                user_nodes.add(node)

        missing_vars = []
        for var in starting_nodes:
            if var not in self.nodes and var not in self.config:
                missing_vars.append(var)
                continue  # collect all missing final variables
            dfs_traverse(self.nodes[var])
        if missing_vars:
            missing_vars_str = ",\n".join(missing_vars)
            raise ValueError(f"Unknown nodes [{missing_vars_str}] requested. Check for typos?")
        return nodes, user_nodes

    def execute(
        self,
        nodes: Collection[node.Node] = None,
        computed: Dict[str, Any] = None,
        overrides: Dict[str, Any] = None,
        inputs: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Executes the DAG, given potential inputs/previously computed components.

        :param nodes: Nodes to compute
        :param computed: Nodes that have already been computed
        :param overrides: Overrides for nodes in the DAG
        :param inputs: Inputs to the DAG -- have to be disjoint from config.
        :return: The result of executing the DAG (a dict of node name to node result)
        """
        if nodes is None:
            nodes = self.get_nodes()
        if inputs is None:
            inputs = {}
        inputs = combine_config_and_inputs(self.config, inputs)
        return execute_subdag(
            nodes=nodes,
            inputs=inputs,
            adapter=self.adapter,
            computed=computed,
            overrides=overrides,
        )
