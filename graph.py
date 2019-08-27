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

logger = logging.getLogger(__name__)


class Node(object):
    """Object representing a node of computation."""

    def __init__(self, name: str, typ: Type, doc_string: str = '', callabl: Callable = None,
                 user_defined: bool = False, input_types: Dict[str, Type] = None):
        """Constructor for our Node object.

        :param name: the name of the function.
        :param typ: the output type of the function.
        :param doc_string: the doc string for the function. Optional.
        :param callabl: the actual function callable.
        :param user_defined: whether this is something someone has to pass in.
        :param input_types: the input parameters and their types.
        """
        self._name = name
        self._type = typ
        if typ is None or typ == inspect._empty:
            raise ValueError(f'Missing type for hint for function {name}. Please add one to fix.')
        self._callable = callabl
        self._doc = doc_string
        self._user_defined = user_defined
        self._dependencies = []
        self._depended_on_by = []

        if not self.user_defined:
            if input_types is not None:
                self._input_types = input_types
            else:
                signature = inspect.signature(callabl)
                self._input_types = {}
                for key, value in signature.parameters.items():
                    if value.annotation == inspect._empty:
                        raise ValueError(f'Missing type hint for {key} in function {name}. Please add one to fix.')
                    self._input_types[key] = value.annotation


    @property
    def documentation(self) -> str:
        return self._doc

    @property
    def input_types(self) -> Dict[str, Type]:
        return self._input_types

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> Any:
        return self._type

    @property
    def callable(self):
        return self._callable

    @property
    def user_defined(self):
        return self._user_defined

    @property
    def dependencies(self) -> List['Node']:
        return self._dependencies

    @property
    def depended_on_by(self) -> List['Node']:
        return self._depended_on_by

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f'<{self._name}>'

    def __eq__(self, other: 'Node'):
        """Want to deeply compare nodes in a custom way.

        Current user is just unit tests. But you never know :)

        Note: we only compare names of dependencies because we don't want infinite recursion.
        """
        return (self._name == other.name and
                self._type == other.type and
                self._doc == other.documentation and
                self.user_defined == other.user_defined and
                [n.name for n in self.dependencies] == [o.name for o in other.dependencies] and
                [n.name for n in self.depended_on_by] == [o.name for o in other.depended_on_by])

    def __ne__(self, other: 'Node'):
        return not self.__eq__(other)


def generate_nodes(fn: Callable, name: str, config: Dict[str, Any]) -> Collection[Node]:
    """Gets a list of nodes from a function. This is meant to be an abstraction between the node
    and the function that it implements. This will end up coordinating with the decorators we build
    to modify nodes.

    :param fn: Function to input.
    :param name: Function name -- will (in some cases) be the name of the node.
    :return: A list of nodes into which this function transforms.
    """
    if hasattr(fn, function_modifiers.NodeExpander.GENERATE_NODES):
        return getattr(fn, function_modifiers.NodeExpander.GENERATE_NODES)(fn, config)
    sig = inspect.signature(fn)
    return [Node(name, sig.return_annotation, fn.__doc__ if fn.__doc__ else '', callabl=fn)]


# kind of hacky for now but it will work
def is_submodule(child: ModuleType, parent: ModuleType):
    return parent.__name__ in child.__name__


def find_functions(function_module: ModuleType) -> List[Tuple[str, Callable]]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the `funcs` imports and grabs all function definitions.
    :return: list of tuples of (func_name, function).
    """

    def valid_fn(fn):
        return (inspect.isfunction(fn)
                and not fn.__name__.startswith('_')
                and is_submodule(inspect.getmodule(fn), function_module))

    return [f for f in inspect.getmembers(function_module, predicate=valid_fn)]


def add_dependency(
        func_node: Node, func_name: str, nodes: Dict[str, Node], param_name: str, param_type: Type):
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
        if not issubclass(required_node.type, param_type):
            raise ValueError(f'Error: {func_name} is expecting {param_name}:{param_type}, but found '
                             f'{param_name}:{required_node.type}. All names & types must match.')
    else:
        # this is a user defined var
        required_node = Node(param_name, param_type, user_defined=True)
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def create_function_graph(*modules: ModuleType, config: Dict[str, Any]) -> Dict[str, Node]:
    """Creates a graph of all available functions & their dependencies.
    :param modules: A set of modules over which one wants to compute the function graph
    :return: list of nodes in the graph.
    If it needs to be more complicated, we'll return an actual networkx graph and get all the rest of the logic for free
    """
    nodes = {}  # name -> Node
    functions = sum([find_functions(module) for module in modules], [])

    # create nodes -- easier to just create this in one loop
    for func_name, f in functions:
        for node in generate_nodes(f, func_name, config):
            if node.name in nodes:
                raise ValueError(f'Cannot define function {node.name} more than once!')
            nodes[node.name] = node
    # add dependencies -- now that all nodes exist, we just run through edges & validate graph.
    for node_name, node in list(nodes.items()):
        for param_name, param_type in node.input_types.items():
            add_dependency(node, node_name, nodes, param_name, param_type)
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

    def get_nodes(self) -> List[Node]:
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

    def get_required_functions(self, final_vars: List[str]) -> Tuple[Set[Node], Set[Node]]:
        """Given our function graph, and a list of desired output variables, returns the subgraph required to compute them.

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

    @staticmethod
    def execute_static(nodes: Collection[Node],
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

        def dfs_traverse(node: Node):
            for n in node.dependencies:
                if n.name not in computed:
                    dfs_traverse(n)

            logger.debug(f'Computing {node.name}.')
            if node.user_defined:
                if node.name not in inputs:
                    raise NotImplementedError(f'{node.name} as expected to be passed in but was not.')
                value = inputs[node.name]
            else:
                if node.name in overrides:
                    computed[node.name] = overrides[node.name]
                    return
                kwargs = {}  # construct signature
                for dependency in node.dependencies:
                    kwargs[dependency.name] = computed[dependency.name]
                value = node.callable(**kwargs)
            computed[node.name] = value

        for final_var_node in nodes:
            dfs_traverse(final_var_node)
        return computed

    def execute(self,
                nodes: Collection[Node] = None,
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
