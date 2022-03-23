"""
This module should not have any real business logic.

It should only house the graph & things required to create and traverse one.

Note: one should largely consider the code in this module to be "private".
"""
import inspect
import logging
import typing
from types import ModuleType
from typing import Type, Dict, Any, Callable, Tuple, Set, Collection, List

import typing_inspect

import hamilton.function_modifiers_base
from hamilton import node
from hamilton.node import NodeSource, DependencyType
from hamilton import base

logger = logging.getLogger(__name__)
BASE_ARGS_FOR_GENERICS = (typing.T,)


# kind of hacky for now but it will work
def is_submodule(child: ModuleType, parent: ModuleType):
    return parent.__name__ in child.__name__


def custom_subclass_check(requested_type: Type[Type], param_type: Type[Type]):
    """This is a custom check around generics & classes. It probably misses a few edge cases.

    We will likely need to revisit this in the future (perhaps integrate with graphadapter?)

    :param requested_type: Candidate subclass
    :param param_type: Type of parameter to check
    :return: Whether or not this is a valid subclass.
    """
    # handles case when someone is using primitives and generics
    requested_origin_type = requested_type
    param_origin_type = param_type
    has_generic = False
    if typing_inspect.is_generic_type(requested_type) or typing_inspect.is_tuple_type(requested_type):
        requested_origin_type = typing_inspect.get_origin(requested_type)
        has_generic = True
    if typing_inspect.is_generic_type(param_type) or typing_inspect.is_tuple_type(param_type):
        param_origin_type = typing_inspect.get_origin(param_type)
        has_generic = True
    if requested_origin_type == param_origin_type:
        if has_generic:  # check the args match or they do not have them defined.
            requested_args = typing_inspect.get_args(requested_type)
            param_args = typing_inspect.get_args(param_type)
            if (requested_args and param_args
                    and requested_args != BASE_ARGS_FOR_GENERICS and param_args != BASE_ARGS_FOR_GENERICS):
                return requested_args == param_args
        return True

    if ((typing_inspect.is_generic_type(requested_type) and typing_inspect.is_generic_type(param_type)) or
            (inspect.isclass(requested_type) and typing_inspect.is_generic_type(param_type))):
        # we're comparing two generics that aren't equal -- check if Mapping vs Dict
        # or we're comparing a class to a generic -- check if Mapping vs dict
        # the precedence is that requested will go into the param_type, so the param_type should be more permissive.
        return issubclass(requested_type, param_type)
    # classes - precedence is that requested will go into the param_type, so the param_type should be more permissive.
    if inspect.isclass(requested_type) and inspect.isclass(param_type) and issubclass(requested_type, param_type):
        return True
    return False


def types_match(adapter: base.HamiltonGraphAdapter,
                param_type: Type[Type],
                required_node_type: Any) -> bool:
    """Checks that we have "types" that "match".

    Matching can be loose here -- and depends on the adapter being used as to what is
    allowed. Otherwise it does a basic equality check.

    :param adapter: the graph adapter to delegate to for one check.
    :param param_type: the parameter type we're checking.
    :param required_node_type: the expected parameter type to validate against.
    :return: True if types are "matching", False otherwise.
    """
    if required_node_type == typing.Any:
        return True
    # type var  -- straight == should suffice. Assume people understand what they're doing with TypeVar.
    elif typing_inspect.is_typevar(required_node_type) or typing_inspect.is_typevar(param_type):
        return required_node_type == param_type
    elif required_node_type == param_type:
        return True
    elif custom_subclass_check(required_node_type, param_type):
        return True
    elif adapter.check_node_type_equivalence(required_node_type, param_type):
        return True
    return False


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
        func_node: node.Node, func_name: str, nodes: Dict[str, node.Node], param_name: str, param_type: Type,
        adapter: base.HamiltonGraphAdapter):
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
            raise ValueError(f'Error: {func_name} is expecting {param_name}:{param_type}, but found '
                             f'{param_name}:{required_node.type}. All names & types must match.')
    else:
        # this is a user defined var
        required_node = node.Node(param_name, param_type, node_source=NodeSource.EXTERNAL)
        nodes[param_name] = required_node
    # add edges
    func_node.dependencies.append(required_node)
    required_node.depended_on_by.append(func_node)


def create_function_graph(*modules: ModuleType, config: Dict[str, Any], adapter: base.HamiltonGraphAdapter) -> Dict[str, node.Node]:
    """Creates a graph of all available functions & their dependencies.
    :param modules: A set of modules over which one wants to compute the function graph
    :param config: Dictionary that we will inspect to get values from in building the function graph.
    :param adapter: The adapter that adapts our node type checking based on the context.
    :return: list of nodes in the graph.
    If it needs to be more complicated, we'll return an actual networkx graph and get all the rest of the logic for free
    """
    nodes = {}  # name -> Node
    functions = sum([find_functions(module) for module in modules], [])

    # create nodes -- easier to just create this in one loop
    for func_name, f in functions:
        for n in hamilton.function_modifiers_base.resolve_nodes(f, config):
            if n.name in config:
                continue  # This makes sure we overwrite things if they're in the config...
            if n.name in nodes:
                raise ValueError(f'Cannot define function {n.name} more than once.'
                                 f' Already defined by function {f}')
            nodes[n.name] = n
    # add dependencies -- now that all nodes exist, we just run through edges & validate graph.
    for node_name, n in list(nodes.items()):
        for param_name, (param_type, _) in n.input_types.items():
            add_dependency(n, node_name, nodes, param_name, param_type, adapter)
    for key in config.keys():
        if key not in nodes:
            nodes[key] = node.Node(key, Any, node_source=NodeSource.EXTERNAL)
    return nodes


def create_graphviz_graph(nodes: Set[node.Node], user_nodes: Set[node.Node], comment: str) -> 'graphviz.Digraph':
    """Helper function to create a graphviz graph.

    :param nodes: The set of computational nodes
    :param user_nodes: The set of nodes that the user is providing inputs for.
    :param comment: The comment to have on the graph.
    :return: a graphviz.Digraph; use this to render/save a graph representation.
    """
    import graphviz
    digraph = graphviz.Digraph(comment=comment)
    for n in nodes:
        digraph.node(n.name, label=n.name)
    for n in user_nodes:
        digraph.node(n.name, label=f'UD: {n.name}')

    for n in list(nodes) + list(user_nodes):
        for d in n.dependencies:
            digraph.edge(d.name, n.name)
    return digraph


def create_networkx_graph(nodes: Set[node.Node], user_nodes: Set[node.Node], name: str) -> 'networkx.DiGraph':
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
        digraph.add_node(n.name, label=f'UD: {n.name}')

    for n in list(nodes) + list(user_nodes):
        for d in n.dependencies:
            digraph.add_edge(d.name, n.name)
    return digraph


class FunctionGraph(object):
    """Note: this object should be considered private until stated otherwise.

    That is, you should not try to build off of it directly without chatting to us first.
    """

    def __init__(self, *modules: ModuleType, config: Dict[str, Any], adapter: base.HamiltonGraphAdapter = None):
        """Initializes a function graph by crawling through modules. Function graph must have a config,
        as the config could determine the shape of the graph.

        :param modules: Modules to crawl for functions
        :param config: this is configuration and/or initial data.
        :param adapter: adapts function building and graph execution for different contexts.
        """
        if adapter is None:
            adapter = base.SimplePythonDataFrameGraphAdapter()

        self._config = config
        self.nodes = create_function_graph(*modules, config=self._config, adapter=adapter)
        self.adapter = adapter

    @property
    def config(self):
        return self._config

    def get_nodes(self) -> List[node.Node]:
        return list(self.nodes.values())

    def display_all(self, output_file_path: str = 'test-output/graph-all.gv', render_kwargs: dict = None):
        """Displays & saves a dot file of the entire DAG structure constructed.

        :param output_file_path: the place to save the files.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        """
        defined_nodes = set()
        user_nodes = set()
        for n in self.nodes.values():
            if n.user_defined:
                user_nodes.add(n)
            else:
                defined_nodes.add(n)
        if render_kwargs is None:
            render_kwargs = {}
        self.display(defined_nodes, user_nodes, output_file_path=output_file_path, render_kwargs=render_kwargs)

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
                ' networkx is required for detecting cycles in the function graph. Install it with:'
                '\n\n  pip install sf-hamilton[visualization] or pip install networkx \n\n'
            )
            return False
        digraph = create_networkx_graph(nodes, user_nodes, 'Dependency Graph')
        cycles = list(networkx.simple_cycles(digraph))
        return cycles

    @staticmethod
    def display(nodes: Set[node.Node],
                user_nodes: Set[node.Node],
                output_file_path: str = 'test-output/graph.gv',
                render_kwargs: dict = None):
        """Function to display the graph represented by the passed in nodes.

        :param nodes: the set of nodes that need to be computed.
        :param user_nodes: the set of inputs that the user provided.
        :param output_file_path: the path where we want to store the a `dot` file + pdf picture.
        :param render_kwargs: kwargs to be passed to the render function to visualize.
        """
        # Check to see if optional dependencies have been installed.
        try:
            import graphviz
        except ModuleNotFoundError:
            logger.exception(
                ' graphviz is required for visualizing the function graph. Install it with:'
                '\n\n  pip install sf-hamilton[visualization] or pip install graphviz \n\n'
            )
            return

        dot = create_graphviz_graph(nodes, user_nodes, 'Dependency Graph')
        kwargs = {'view': True}
        if kwargs and isinstance(render_kwargs, dict):
            kwargs.update(render_kwargs)
        dot.render(output_file_path, **kwargs)

    def get_impacted_nodes(self, var_changes: List[str]) -> Set[node.Node]:
        """Given our function graph, and a list of nodes that are changed,
        returns the subgraph that they will impact.

        :param var_changes: the list of nodes that will change.
        :return: A set of all changed nodes.
        """
        nodes, user_nodes = self.directional_dfs_traverse(lambda n: n.depended_on_by, starting_nodes=var_changes)
        return nodes

    def get_upstream_nodes(self, final_vars: List[str], runtime_inputs: Dict[str, Any] = None) -> Tuple[Set[node.Node], Set[node.Node]]:
        """Given our function graph, and a list of desired output variables, returns the subgraph required to compute them.

        :param final_vars: the list of node names we want.
        :param runtime_inputs: runtime inputs to the DAG -- if not provided we will assume we're running at compile-time.
        Everything would then be required (even though it might be marked as optional), as we want to crawl the whole DAG.
        If we're at runtime, we want to just use the nodes that are provided, and not count the optional ones that are not.
        :return: a tuple of sets:
            - set of all nodes.
            - subset of nodes that human input is required for.
        """

        def next_nodes_function(n: node.Node) -> List[node.Node]:
            if runtime_inputs is None:
                return n.dependencies
            deps = []
            for dep in n.dependencies:
                # If inputs is None, we want to assume its required, as it is a compile-time dependency
                if dep.user_defined and dep.name not in runtime_inputs and dep.name not in self.config:
                    _, dependency_type = n.input_types[dep.name]
                    if dependency_type == DependencyType.OPTIONAL:
                        continue
                deps.append(dep)
            return deps

        return self.directional_dfs_traverse(next_nodes_function, starting_nodes=final_vars)

    def directional_dfs_traverse(self, next_nodes_fn: Callable[[node.Node], Collection[node.Node]], starting_nodes: List[str]):
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
            missing_vars_str = ',\n'.join(missing_vars)
            raise ValueError(f'Unknown nodes [{missing_vars_str}] requested. Check for typos?')
        return nodes, user_nodes

    @staticmethod
    def execute_static(nodes: Collection[node.Node],
                       inputs: Dict[str, Any],
                       adapter: base.HamiltonGraphAdapter,
                       computed: Dict[str, Any] = None,
                       overrides: Dict[str, Any] = None):
        """Executes computation on the given graph, inputs, and memoized computation.

        Effectively this is a "private" function and should be viewed as such.

        To override a value, utilize `overrides`.
        To pass in a value to ensure we don't compute data twice, use `computed`.
        Don't use `computed` to override a value, you will not get the results you expect.

        :param nodes: the graph to traverse for execution.
        :param inputs: the inputs provided. These will only be called if a node is "user-defined"
        :param adapter: object that adapts execution based on context it knows about.
        :param computed: memoized storage to speed up computation. Usually an empty dict.
        :param overrides: any inputs we want to user to override actual computation
        :return: the passed in dict for memoized storage.
        """

        if overrides is None:
            overrides = {}
        if computed is None:
            computed = {}

        def dfs_traverse(node: node.Node, dependency_type: DependencyType = DependencyType.REQUIRED):
            if node.name in computed:
                return
            if node.name in overrides:
                computed[node.name] = overrides[node.name]
                return
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
                kwargs = {}  # construct signature
                for dependency in node.dependencies:
                    if dependency.name in computed:
                        kwargs[dependency.name] = computed[dependency.name]
                try:
                    value = adapter.execute_node(node, kwargs)
                except Exception as e:
                    logger.exception(f'Node {node.name} encountered an error')
                    raise
            computed[node.name] = value

        for final_var_node in nodes:
            dfs_traverse(final_var_node)
        return computed

    @staticmethod
    def combine_config_and_inputs(config: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Validates and combines config and inputs, ensuring that they're mutually disjoint.
        :param config: Config to construct, run the DAG with.
        :param inputs: Inputs to run the DAG on at runtime
        :return: The combined set of inputs to the DAG.
        :raises ValueError: if they are not disjoint
        """
        duplicated_inputs = [key for key in inputs if key in config]
        if len(duplicated_inputs) > 0:
            raise ValueError(f'The following inputs are present in both config and inputs. They must be mutually disjoint. {duplicated_inputs}')
        return {**config, **inputs}

    def execute(self,
                nodes: Collection[node.Node] = None,
                computed: Dict[str, Any] = None,
                overrides: Dict[str, Any] = None,
                inputs: Dict[str, Any] = None
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
        return FunctionGraph.execute_static(
            nodes=nodes,
            inputs=FunctionGraph.combine_config_and_inputs(self.config, inputs),
            adapter=self.adapter,
            computed=computed,
            overrides=overrides,
        )
