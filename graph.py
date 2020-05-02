""""
This module should not have any real business logic.

It should only be able the graph & things required to create and traverse one.
"""
import inspect
import logging
from types import ModuleType
from typing import Type, Dict, Any, Callable, Tuple, Set, Collection, List

import graphviz
import networkx

from hamilton import function_modifiers
from hamilton import node
from hamilton.node import NodeSource, DependencyType

logger = logging.getLogger(__name__)


# kind of hacky for now but it will work
def is_submodule(child: ModuleType, parent: ModuleType):
    return parent.__name__ in child.__name__


def find_functions(function_module: ModuleType) -> List[Tuple[str, Callable]]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the function module and grabs all function definitions.
    :return: list of tuples of (func_name, function).
    """

    def valid_fn(fn):
        return (inspect.isfunction(fn)
                and not fn.__name__.startswith('_')
                and is_submodule(inspect.getmodule(fn), function_module))

    return [f for f in inspect.getmembers(function_module, predicate=valid_fn)]


def add_dependency(
        func_node: node.Node, func_name: str, nodes: Dict[str, node.Node], param_name: str, param_type: Type):
    """Adds dependencies to the node objects.

    This will add user defined inputs to the dictionary of nodes in the graph.

    :param func_node: the node we're pulling dependencies from.
    :param func_name: the name of the function we're inspecting.
    :param nodes: nodes representing the graph. This function mutates this object and underlying objects.
    :param param_name: the parameter name we're looking for/adding as a dependency.
    :param param_type: the type of the parameter.
    """
    if param_name in nodes:
        # validate types match
        required_node = nodes[param_name]
        if param_type == dict and issubclass(required_node.type, Dict):  # python3.7 changed issubclass behavior
            pass
        elif param_type == list and issubclass(required_node.type, List):  # python3.7 changed issubclass behavior
            pass
        elif issubclass(required_node.type, param_type):
            pass
        else:
            raise ValueError(f'Error: {func_name} is expecting {param_name}:{param_type}, but found '
                             f'{param_name}:{required_node.type}. All names & types must match.')
    else:
        # this is a user defined var
        required_node = node.Node(param_name, param_type, node_source=NodeSource.EXTERNAL)
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def create_function_graph(*modules: ModuleType, config: Dict[str, Any]) -> Dict[str, node.Node]:
    """Creates a graph of all available functions & their dependencies.
    :param modules: A set of modules over which one wants to compute the function graph
    :return: list of nodes in the graph.
    If it needs to be more complicated, we'll return an actual networkx graph and get all the rest of the logic for free
    """
    nodes = {}  # name -> Node
    functions = sum([find_functions(module) for module in modules], [])

    # create nodes -- easier to just create this in one loop
    for func_name, f in functions:
        for n in function_modifiers.resolve_nodes(f, config):
            if n.name in config:
                continue # This makes sure we overwrite things if they're in the config...
            if n.name in nodes:
                raise ValueError(f'Cannot define function {node.name} more than once.'
                                 f' Already defined by function {f}')
            nodes[n.name] = n
    # add dependencies -- now that all nodes exist, we just run through edges & validate graph.
    for node_name, n in list(nodes.items()):
        for param_name, (param_type, _) in n.input_types.items():
            add_dependency(n, node_name, nodes, param_name, param_type)
    for key in config.keys():
        if key not in nodes:
            nodes[key] = node.Node(key, Any, node_source=NodeSource.EXTERNAL)
    return nodes


class FunctionGraph(object):
    def __init__(self, *modules: ModuleType, config: Dict[str, Any]):
        """Initializes a function graph by crawling through modules. Function graph must have a config,
        as the config could determine the shape of the graph.

        :param modules: Modules to crawl for functions
        :param config:
        """
        self._config = config
        self.nodes = create_function_graph(*modules, config=self._config)

    @property
    def config(self):
        return self._config

    def get_nodes(self) -> List[node.Node]:
        return list(self.nodes.values())

    def display_all(self, output_file_path: str = 'test-output/graph-all.gv'):
        """Displays the entire DAG structure constructed.

        :param output_file_path: the place to save the files.
        """
        defined_nodes = set()
        user_nodes = set()
        for n in self.nodes.values():
            if n.user_defined:
                user_nodes.add(n)
            else:
                defined_nodes.add(n)
        self.display(defined_nodes, user_nodes, output_file_path=output_file_path)

    @staticmethod
    def display(nodes: Set[node.Node], user_nodes: Set[node.Node], output_file_path: str = 'test-output/graph.gv'):
        """Function to display the graph represented by the passed in nodes.

        Just because it is easy, we also through in a check for cycles.

        :param final_vars: the final vars we want -- else all if None.
        :param output_file_path: the path where we want to store the a `dot` file + pdf picture.
        """
        dot = graphviz.Digraph(comment='Dependency Graph')

        for n in nodes:
            dot.node(n.name, label=n.name)
        for n in user_nodes:
            dot.node(n.name, label=f'UD: {n.name}')

        for n in list(nodes) + list(user_nodes):
            for d in n.dependencies:
                dot.edge(d.name, n.name)

        dot.render(output_file_path, view=True)
        ntx_graph = networkx.DiGraph(networkx.drawing.nx_agraph.read_dot(output_file_path))
        cycles = list(networkx.simple_cycles(ntx_graph))
        if cycles:
            raise ValueError(f'Error cycle(s) detected:{cycles}')
        else:
            logger.info('No cycles detected')

    def get_impacted_nodes(self, var_changes: List[str]) -> Set[node.Node]:
        """Given our function graph, and a list of nodes that are changed,
        returns the subgraph that they will impact.

        :param var_changes: the list of nodes that will change.
        :return: A set of all changed nodes.
        """
        nodes, user_nodes = self.directional_dfs_traverse(lambda n: n.depended_on_by, starting_nodes=var_changes)
        return nodes

    def get_required_functions(self, final_vars: List[str]) -> Tuple[Set[node.Node], Set[node.Node]]:
        """Given our function graph, and a list of desired output variables, returns the subgraph required to compute them.

        :param final_vars: the list of node names we want.
        :return: a tuple of sets:
            - set of all nodes.
            - subset of nodes that human input is required for.
        """
        return self.directional_dfs_traverse(lambda n: n.dependencies, starting_nodes=final_vars)

    def directional_dfs_traverse(self, next_nodes_fn: Callable[[node.Node], Collection[node.Node]], starting_nodes: List[str]):
        nodes = set()
        user_nodes = set()
        def dfs_traverse(node: node.Node):
            for n in next_nodes_fn(node):
                if n not in nodes:
                    dfs_traverse(n)
            nodes.add(node)
            if node.user_defined:
                user_nodes.add(node)

        missing_vars = []
        for var in starting_nodes:
            if var not in self.nodes and var not in self.config:
                missing_vars.append(var)
                continue  # collect all missing final variables
            dfs_traverse(self.nodes[var])
        if missing_vars:
            missing_vars_str = ',\n'.join(missing_vars)
            raise ValueError(f'Unknown nodes [{missing_vars_str}] requested. Check for typos?')
        return nodes, user_nodes

    @staticmethod
    def execute_static(nodes: Collection[node.Node],
                       inputs: Dict[str, Any],
                       computed: Dict[str, Any] = None,
                       overrides: Dict[str, Any] = None):
        """Executes computation on the given graph, inputs, and memoized computation.
                To override a value, utilize `overrides`.
                To pass in a value to ensure we don't compute data twice, use `computed`.
                Don't use `computed` to override a value, you will not get the results you expect.

                :param nodes: the graph to traverse for execution.
                :param inputs: the inputs provided. These will only be called if a node is "user-defined"
                :param computed: memoized storage to speed up computation. Usually an empty dict.
                :param overrides: any inputs we want to user to override actual computation
                :return: the passed in dict for memoized storage.
                """

        if overrides is None:
            overrides = {}
        if computed is None:
            computed = {}

        def dfs_traverse(node: node.Node, dependency_type: DependencyType=DependencyType.REQUIRED):
            for n in node.dependencies:

                if n.name not in computed:
                    _, node_dependency_type = node.input_types[n.name]
                    dfs_traverse(n, node_dependency_type)

            logger.debug(f'Computing {node.name}.')
            if node.user_defined:
                if node.name not in inputs:
                    if dependency_type != DependencyType.OPTIONAL:
                        raise NotImplementedError(f'{node.name} was expected to be passed in but was not.')
                    return
                value = inputs[node.name]
            else:
                if node.name in overrides:
                    computed[node.name] = overrides[node.name]
                    return
                kwargs = {}  # construct signature
                for dependency in node.dependencies:
                    if dependency.name in computed:
                        kwargs[dependency.name] = computed[dependency.name]
                try:
                    value = node.callable(**kwargs)
                except Exception as e:
                    logger.exception(f"Node {node.name} encountered an error")
                    raise
            computed[node.name] = value

        for final_var_node in nodes:
            dfs_traverse(final_var_node)
        return computed

    def execute(self,
                nodes: Collection[node.Node] = None,
                computed: Dict[str, Any] = None,
                overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        if nodes is None:
            nodes = self.get_nodes()
        return FunctionGraph.execute_static(
            nodes=nodes,
            inputs=self.config,
            computed=computed,
            overrides=overrides
        )
