"""
This module should not have any real business logic.

It should only house the graph & things required to create and traverse one.

Note: one should largely consider the code in this module to be "private".
"""
import logging
from enum import Enum
from types import ModuleType
from typing import Any, Callable, Collection, Dict, FrozenSet, List, Optional, Set, Tuple, Type

from hamilton import base, node
from hamilton.execution import graph_functions
from hamilton.execution.graph_functions import combine_config_and_inputs, execute_subdag
from hamilton.function_modifiers import base as fm_base
from hamilton.graph_utils import find_functions
from hamilton.htypes import get_type_as_string, types_match
from hamilton.node import Node

logger = logging.getLogger(__name__)


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
        required_node: Node = nodes[param_name]
        types_do_match = types_match(adapter, param_type, required_node.type)
        if not types_do_match and required_node.user_defined:
            # check the case that two input type expectations are compatible, e.g. is one a subset of the other
            # this could be the case when one is a union and the other is a subset of that union
            # which is fine for inputs. If they are not compatible, we raise an error.
            types_are_compatible = types_match(adapter, required_node.type, param_type)
            if not types_are_compatible:
                raise ValueError(
                    f"Error: Two or more functions are requesting {param_name}, but have incompatible types. "
                    f"{func_name} requires {param_name} to be {param_type}, but found another function(s) "
                    f"{[f.__name__ for f in required_node.originating_functions]} that minimally require {param_name} "
                    f"as {required_node.type}. Please fix this by ensuring that all functions requesting {param_name} "
                    f"have compatible types. If you believe they are equivalent, please reach out to the developers. "
                    f"Note that, if you have types that are equivalent for your purposes, you can create a "
                    f"graph adapter that checks the types against each other in a more lenient manner."
                )
            else:
                # replace the current type with this "tighter" type.
                required_node.set_type(param_type)
                # add to the originating functions
                for og_func in func_node.originating_functions:
                    required_node.add_originating_function(og_func)
        elif not types_do_match:
            raise ValueError(
                f"Error: {func_name} is expecting {param_name}:{param_type}, but found "
                f"{param_name}:{required_node.type}. \nHamilton does not consider these types to be "
                f"equivalent. If you believe they are equivalent, please reach out to the developers. "
                f"Note that, if you have types that are equivalent for your purposes, you can create a "
                f"graph adapter that checks the types against each other in a more lenient manner."
            )
    else:
        # this is a user defined var, i.e. an input to the graph.
        required_node = node.Node(
            param_name,
            param_type,
            node_source=node.NodeType.EXTERNAL,
            originating_functions=func_node.originating_functions,
        )
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def update_dependencies(
    nodes: Dict[str, node.Node],
    adapter: base.HamiltonGraphAdapter,
    reset_dependencies: bool = True,
):
    """Adds dependencies to a dictionary of nodes. If in_place is False,
    it will deepcopy the dict + nodes and return that. Otherwise it will
    mutate + return the passed-in dict + nodes.

    Note: this will add in "input" nodes if they are not already present.

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

    # create non-input nodes -- easier to just create this in one loop
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
    # add dependencies -- now that all nodes except input nodes, we just run through edges & validate graph.
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
    show_legend: bool = True,
    orient: str = "LR",
    hide_inputs: bool = False,
    deduplicate_inputs: bool = False,
) -> "graphviz.Digraph":  # noqa: F821
    """Helper function to create a graphviz graph.

    :param nodes: The set of computational nodes
    :param comment: The comment to have on the graph.
    :param graphviz_kwargs: kwargs to pass to create the graph.
        e.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
    :param node_modifiers: A dictionary of node names to dictionaries of node attributes to modify.
    :param strictly_display_only_nodes_passed_in: If True, only display the nodes passed in. Else defaults to displaying
        also what nodes a node depends on (i.e. all nodes that feed into it).
    :param show_legend: If True, add a legend to the visualization based on the DAG's nodes.
    :param orient: `LR` stands for "left to right". Accepted values are TB, LR, BT, RL.
        `orient` will be overwridden by the value of `graphviz_kwargs['graph_attr']['rankdir']`
        see (https://graphviz.org/docs/attr-types/rankdir/)
    :param hide_inputs: If True, no input nodes are displayed.
    :param deduplicate_inputs: If True, remove duplicate input nodes.
        Can improve readability depending on the specifics of the DAG.
    :return: a graphviz.Digraph; use this to render/save a graph representation.
    """
    PATH_COLOR = "red"

    import graphviz

    def _get_node_label(
        n: node.Node,
        name: Optional[str] = None,
        type_string: Optional[str] = None,
    ) -> str:
        """Get a graphviz HTML-like node label. It uses the DAG node
        name and type but values can be overridden. Overriding is currently
        used for materializers since `type_` is stored in n._tags.

        ref: https://graphviz.org/doc/info/shapes.html#html
        """
        name = n.name if name is None else name
        if type_string is None:
            type_string = get_type_as_string(n.type) if get_type_as_string(n.type) else ""

        return f"<<b>{name}</b><br /><br /><i>{type_string}</i>>"

    def _get_input_label(input_nodes: FrozenSet[node.Node]) -> str:
        """Get a graphviz HTML-like node label formatted aspyer a table.
        Each row is a different input node with one column containing
        the name and the other the type.
        ref: https://graphviz.org/doc/info/shapes.html#html
        """
        rows = []
        for dep in input_nodes:
            name = dep.name
            type_string = get_type_as_string(dep.type) if get_type_as_string(dep.type) else ""
            rows.append(f"<tr><td>{name}</td><td>{type_string}</td></tr>")
        return f"<<table border=\"0\">{''.join(rows)}</table>>"

    def _get_node_type(n: node.Node) -> str:
        """Get the node type of a DAG node.

        Input: is external, doesn't originate from a function, functions depend on it
        Config: is external, doesn't originate from a function, no function depedends on it
        Function: others
        """
        if n._node_source == node.NodeType.EXTERNAL and n._depended_on_by:
            return "input"
        elif (
            n._node_source == node.NodeType.EXTERNAL
            and n._originating_functions is None
            and not n._depended_on_by
        ):
            return "config"
        else:
            return "function"

    def _get_node_style(node_type: str) -> Dict[str, str]:
        """Get the style of a node type.
        Graphviz needs values to be strings.
        """
        fontname = "Helvetica"

        if node_type == "config":
            node_style = dict(
                shape="note",
                style="",
                fontname=fontname,
            )
        elif node_type == "input":
            node_style = dict(
                shape="rectangle",
                margin="0.15",
                style="dashed",
                fontname=fontname,
            )
        elif node_type == "materializer":
            node_style = dict(
                shape="cylinder",
                margin="0.15,0.1",
                fontname=fontname,
            )
        else:  # this is a function or else
            node_style = dict(
                shape="rectangle",
                margin="0.15",
                style="rounded,filled",
                fillcolor="#b4d8e4",
                fontname=fontname,
            )

        return node_style

    def _get_function_modifier_style(modifier: str) -> Dict[str, str]:
        """Get the style of a modifier. The dictionary returned
        is used to overwrite values of the base node style.
        Graphviz needs values to be strings.
        """
        if modifier == "output":
            modifier_style = dict(fillcolor="#FFC857")
        elif modifier == "collect":
            modifier_style = dict(peripheries="2", color="#EA5556")
        elif modifier == "expand":
            modifier_style = dict(peripheries="2", color="#56E39F")
        elif modifier == "override":
            modifier_style = dict(style="filled,diagonals")
        elif modifier == "materializer":
            modifier_style = dict(shape="cylinder")
        else:
            modifier_style = dict()

        return modifier_style

    def _get_edge_style(from_type: str, to_type: str) -> Dict:
        """

        Graphviz needs values to be strings.
        """
        edge_style = dict()

        if from_type == "expand":
            edge_style.update(
                dir="both",
                arrowhead="crow",
                arrowtail="none",
            )

        if to_type == "collect":
            edge_style.update(dir="both", arrowtail="crow")

        return edge_style

    def _get_legend(node_types: Set[str]):
        """Create a visualization legend as a graphviz subgraph. The legend includes the
        node types and modifiers presente in the visualization.
        """
        legend_subgraph = graphviz.Digraph(
            name="cluster__legend",  # needs to start with `cluster` for graphviz layout
            graph_attr=dict(
                label="Legend",
                rank="same",  # makes the legend perpendicular to the main DAG
                fontname="helvetica",
            ),
        )

        sorted_types = [
            "config",
            "input",
            "function",
            "output",
            "materializer",
            "override",
            "expand",
            "collect",
        ]

        for node_type in sorted(node_types, key=lambda t: sorted_types.index(t)):
            node_style = _get_node_style(node_type)
            modifier_style = _get_function_modifier_style(node_type)
            node_style.update(**modifier_style)
            legend_subgraph.node(name=node_type, **node_style)

        return legend_subgraph

    # handle default values in nested dict
    digraph_attr = dict(
        comment=comment,
        graph_attr=dict(
            rankdir=orient,
            ranksep="0.4",
            compound="true",
            concentrate="true",
        ),
    )
    # we need to update the graph_attr dict instead of overwriting it
    # so that means we need to handle nested dicts, e.g. graph_attr.
    for g_key, g_value in graphviz_kwargs.items():
        if isinstance(g_value, dict):
            digraph_attr[g_key].update(**g_value)
        else:
            digraph_attr[g_key] = g_value
    digraph = graphviz.Digraph(**digraph_attr)

    # create nodes
    seen_node_types = set()
    for n in nodes:
        label = _get_node_label(n)
        node_type = _get_node_type(n)
        seen_node_types.add(node_type)
        if node_type == "input":
            continue

        node_style = _get_node_style(node_type)

        # prefer having the conditions explicit for now since they rely on
        # heterogeneous VisualizationNodeModifiers and node.Node.node_role.
        # Otherwise, it's difficult to manage seen nodes and the legend.
        if n.node_role == node.NodeType.EXPAND:
            modifier_style = _get_function_modifier_style("expand")
            node_style.update(**modifier_style)
            seen_node_types.add("expand")

        if n.node_role == node.NodeType.COLLECT:
            modifier_style = _get_function_modifier_style("collect")
            node_style.update(**modifier_style)
            seen_node_types.add("collect")

        if n._tags.get("hamilton.data_saver"):
            materializer_type = n._tags["hamilton.data_saver.classname"]
            label = _get_node_label(n, type_string=materializer_type)
            modifier_style = _get_function_modifier_style("materializer")
            node_style.update(**modifier_style)
            seen_node_types.add("materializer")

        if node_modifiers.get(n.name):
            modifiers = node_modifiers[n.name]
            if VisualizationNodeModifiers.IS_OUTPUT in modifiers:
                modifier_style = _get_function_modifier_style("output")
                node_style.update(**modifier_style)
                seen_node_types.add("output")

            if VisualizationNodeModifiers.IS_OVERRIDE in modifiers:
                modifier_style = _get_function_modifier_style("override")
                node_style.update(**modifier_style)
                seen_node_types.add("override")

            if VisualizationNodeModifiers.IS_PATH in modifiers:
                # use PATH_COLOR only if no color applied, edges provide enough clarity
                # currently, only EXPAND and COLLECT use the `color` attribue
                node_style["color"] = node_style.get("color", PATH_COLOR)

        digraph.node(n.name, label=label, **node_style)

    # create edges
    input_sets = dict()
    for n in nodes:
        to_type = "collect" if n.node_role == node.NodeType.COLLECT else ""
        to_modifiers = node_modifiers.get(n.name, set())

        input_nodes = set()
        for d in n.dependencies:
            if strictly_display_only_nodes_passed_in and d not in nodes:
                continue

            dependency_type = _get_node_type(d)
            # input nodes and edges are gathered instead of drawn
            # they are drawn later, see below
            if dependency_type == "input":
                input_nodes.add(d)
                continue

            from_type = "expand" if d.node_role == node.NodeType.EXPAND else ""
            dependency_modifiers = node_modifiers.get(d.name, set())
            edge_style = _get_edge_style(from_type, to_type)
            if (
                VisualizationNodeModifiers.IS_PATH in dependency_modifiers
                and VisualizationNodeModifiers.IS_PATH in to_modifiers
            ):
                edge_style["color"] = PATH_COLOR

            digraph.edge(d.name, n.name, **edge_style)

        # skip input node creation
        if hide_inputs:
            continue

        # draw input nodes if at least 1 exist
        if len(input_nodes) > 0:
            input_node_name = f"_{n.name}_inputs"

            # following block is for input node deduplication
            input_nodes = frozenset(input_nodes)
            if input_sets.get(input_nodes):
                existing_input_name = input_sets[input_nodes]
                digraph.edge(existing_input_name, n.name)
                continue

            # allow duplicate input nodes by never storing keys
            if deduplicate_inputs:
                input_sets[input_nodes] = input_node_name

            # create input node
            node_label = _get_input_label(input_nodes)
            node_style = _get_node_style("input")
            digraph.node(name=input_node_name, label=node_label, **node_style)
            # create edge for input node
            digraph.edge(input_node_name, n.name)

    if show_legend:
        digraph.subgraph(_get_legend(seen_node_types))
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
        *modules: ModuleType,
        config: Dict[str, Any],
        adapter: base.HamiltonGraphAdapter = None,
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
        show_legend: bool = True,
        orient: str = "LR",
        hide_inputs: bool = False,
        deduplicate_inputs: bool = False,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Displays & saves a dot file of the entire DAG structure constructed.

        :param output_file_path: the place to save the files.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: kwargs to be passed to the graphviz graph object to configure it.
            e.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :param show_legend: If True, add a legend to the visualization based on the DAG's nodes.
        :param orient: `LR` stands for "left to right". Accepted values are TB, LR, BT, RL.
            `orient` will be overwridden by the value of `graphviz_kwargs['graph_attr']['rankdir']`
            see (https://graphviz.org/docs/attr-types/rankdir/)
        :param hide_inputs: If True, no input nodes are displayed.
        :param deduplicate_inputs: If True, remove duplicate input nodes.
            Can improve readability depending on the specifics of the DAG.
        :return: the graphviz graph object if it was created. None if not.
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
            show_legend=show_legend,
            orient=orient,
            hide_inputs=hide_inputs,
            deduplicate_inputs=deduplicate_inputs,
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
        show_legend: bool = True,
        orient: str = "LR",
        hide_inputs: bool = False,
        deduplicate_inputs: bool = False,
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
        :param show_legend: If True, add a legend to the visualization based on the DAG's nodes.
        :param orient: `LR` stands for "left to right". Accepted values are TB, LR, BT, RL.
            `orient` will be overridden by the value of `graphviz_kwargs['graph_attr']['rankdir']`
            see (https://graphviz.org/docs/attr-types/rankdir/)
        :param hide_inputs: If True, no input nodes are displayed.
        :param deduplicate_inputs: If True, remove duplicate input nodes.
            Can improve readability depending on the specifics of the DAG.
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
            show_legend,
            orient,
            hide_inputs,
            deduplicate_inputs,
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
        self,
        next_nodes_fn: Callable[[node.Node], Collection[node.Node]],
        starting_nodes: List[str],
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
