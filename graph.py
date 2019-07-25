""""
This module should not have any real business logic.

It should only be able the graph & things required to create and traverse one.
"""
import inspect
import logging
import typing
from types import ModuleType

import graphviz
import networkx

logger = logging.getLogger(__name__)


class Node(object):
    """Object representing a node of computation."""

    def __init__(self, name: str, typ: typing.Any, callable: typing.Callable = None, user_defined: bool = False):
        self._name = name
        self._type = typ
        self._callable = callable
        self._user_defined = user_defined
        self._dependencies = []
        self._depended_on_by = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> typing.Any:
        return self._type

    @property
    def callable(self):
        return self._callable

    @property
    def user_defined(self):
        return self._user_defined

    @property
    def dependencies(self) -> list:
        return self._dependencies

    @property
    def depended_on_by(self) -> list:
        return self._depended_on_by

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f'<{self._name}>'


def find_functions(function_module: ModuleType) -> typing.List[typing.Callable]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the `funcs` imports and grabs all function definitions.
    :return: list of dicts of func_name -> function.
    """
    return [f for f in inspect.getmembers(function_module) if inspect.isfunction(f[1]) and not f[0].startswith('_')]


def add_dependency(
        func_node: Node, func_name: str, nodes: typing.Dict[str, Node], param_name: str, param_type: typing.Any):
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
        if required_node.type != param_type.annotation:
            raise ValueError(f'Error: {func_name} is expecting {param_name}:{param_type.annotation}, but found '
                             f'{param_name}:{required_node.type}. All names & types must match.')
    else:
        # this is a user defined var
        required_node = Node(param_name, param_type.annotation, user_defined=True)
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def create_function_graph(module: ModuleType) -> typing.Dict[str, Node]:
    """Creates a graph of all available functions & their depdendencies.

    :return: list of nodes in the graph.
    If it needs to be more complicated, we'll return an actual networkx graph and get all the rest of the logic for free
    """
    nodes = {}  # name -> Node
    functions = find_functions(module)

    # create nodes -- easier to just create this in one loop
    for func_name, f in functions:
        sig = inspect.signature(f)
        n = Node(func_name, sig.return_annotation, callable=f)
        if func_name in nodes:
            raise ValueError(f'Cannot define function {func_name} more than once!')
        nodes[func_name] = n

    # add dependencies -- now that all nodes exist, we just run through edges & validate graph.
    for func_name, f in functions:
        func_node = nodes[func_name]
        sig = inspect.signature(f)
        for param_name, param_type in sig.parameters.items():
            add_dependency(func_node, func_name, nodes, param_name, param_type)
    return nodes


class FunctionGraph(object):
    def __init__(self, module: ModuleType):
        self.nodes = create_function_graph(module)

    def get_nodes(self) -> typing.List[Node]:
        return list(self.nodes.values())

    def display(self, output_file_path: str = 'test-output/graph.gv'):
        """Function to display the graph represented by the passed in nodes.

            Just because it is easy, we also through in a check for cycles.

            :param graph: the DAG we want to display
            :param output_file_path: the path where we want to store the a `dot` file + pdf picture.
            """
        dot = graphviz.Digraph(comment='Dependency Graph')
        for n in self.nodes.values():
            label = f'UD: {n.name}' if n.user_defined else n.name
            dot.node(n.name, label)

        for n in self.nodes.values():
            for d in n.dependencies:
                dot.edge(d.name, n.name)

        dot.render(output_file_path, view=True)
        ntx_graph = networkx.DiGraph(networkx.drawing.nx_agraph.read_dot(output_file_path))
        cycles = list(networkx.simple_cycles(ntx_graph))
        if cycles:
            raise ValueError(f'Error cycle(s) detected:{cycles}')
        else:
            logger.info('No cycles detected')

    def get_required_functions(self, final_vars: typing.List[str]) -> typing.Tuple[typing.Set[Node], typing.Set[Node]]:
        """Given our function graph, and a list of desired output variables, returns the subgraph required to compute them.

        :param function_graph: The function graph that we're using
        :param final_vars: the list of node names we want.
        :return: a tuple of sets:
            - set of all nodes.
            - subset of nodes that human input is required for.
        """
        nodes = set()
        user_nodes = set()

        def dfs_traverse(node: Node):
            for n in node.dependencies:
                if n not in nodes:
                    dfs_traverse(n)
            nodes.add(node)
            if node.user_defined:
                user_nodes.add(node)

        for final_var in final_vars:
            if final_var not in self.nodes:
                raise ValueError(f'Unknown node {final_var} requested. Check for typos?')
            dfs_traverse(self.nodes[final_var])

        return nodes, user_nodes

    def execute(self,
                nodes: typing.Collection[Node],
                inputs: typing.Dict[str, typing.Any],
                computed: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        """Executes computation on the given graph, inputs, and memoized computation.

        :param nodes: the graph to traverse for execution.
        :param inputs: the inputs provided.
        :param computed: memoized storage to speed up computation. Usually an empty dict.
        :return: the passed in dict for memoized storage.
        """

        def dfs_traverse(node: Node):
            for n in node.dependencies:
                if n.name not in computed:
                    dfs_traverse(n)

            logger.info(f'Computing {node.name}.')
            if node.user_defined:
                value = inputs[node.name]
            else:
                kwargs = {}  # construct signature
                for dependency in node.dependencies:
                    kwargs[dependency.name] = computed[dependency.name]
                value = node.callable(**kwargs)
            computed[node.name] = value

        for final_var_node in nodes:
            dfs_traverse(final_var_node)
        return computed
