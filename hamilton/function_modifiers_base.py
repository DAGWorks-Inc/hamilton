import abc
from typing import Any, Callable, Collection, Dict, Tuple

from hamilton import node


def sanitize_function_name(name: str) -> str:
    """Sanitizes the function name to use.
    Note that this is a slightly leaky abstraction, but this is really just a single case in which we want to strip out
    dunderscores. This will likely change over time, but for now we need a way for a decorator to know about the true
    function name without having to rely on the decorator order. So, if you want the function name of the function you're
    decorating, call this first.

    :param name: Function name
    :return: Sanitized version.
    """
    last_dunder_index = name.rfind("__")
    return name[:last_dunder_index] if last_dunder_index != -1 else name


class NodeTransformLifecycle(abc.ABC):
    """Base class to represent the decorator lifecycle. Common among all node decorators."""

    @classmethod
    @abc.abstractmethod
    def get_lifecycle_name(cls) -> str:
        """Gives the lifecycle name of the node decorator. Unique to the class, will likely not be overwritten by subclasses.
        Note that this is coupled with the resolve_node() function below.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def allows_multiple(cls) -> bool:
        """Whether or not multiple of these decorators are allowed.

        :return: True if multiple decorators are allowed else False
        """
        pass

    @abc.abstractmethod
    def validate(self, fn: Callable):
        """Validates the decorator against the function

        :param fn: Function to validate against
        :return: Nothing, raises exception if not valid.
        """
        pass

    def __call__(self, fn: Callable):
        """Calls the decorator by adding attributes using the get_lifecycle_name string.
        These attributes are the pointer to the decorator object itself, and used later in resolve_nodes below.

        :param fn: Function to decorate
        :return: The function again, with the desired properties.
        """
        self.validate(fn)
        lifecycle_name = self.__class__.get_lifecycle_name()
        if hasattr(fn, self.get_lifecycle_name()):
            if not self.allows_multiple():
                raise ValueError(
                    f"Got multiple decorators for decorator @{self.__class__}. Only one allowed."
                )
            curr_value = getattr(fn, lifecycle_name)
            setattr(fn, lifecycle_name, curr_value + [self])
        else:
            setattr(fn, lifecycle_name, [self])
        return fn


class NodeResolver(NodeTransformLifecycle):
    """Decorator to resolve a nodes function. Can modify anything about the function and is run at DAG creation time."""

    @abc.abstractmethod
    def resolve(self, fn: Callable, configuration: Dict[str, Any]) -> Callable:
        """Determines what a function resolves to. Returns None if it should not be included in the DAG.

        :param fn: Function to resolve
        :param configuration: DAG config
        :return: A name if it should resolve to something. Otherwise None.
        """
        pass

    @abc.abstractmethod
    def validate(self, fn):
        """Validates that the function can work with the function resolver.

        :param fn: Function to validate
        :return: nothing
        :raises InvalidDecoratorException: if the function is not valid for this decorator
        """
        pass

    @classmethod
    def get_lifecycle_name(cls) -> str:
        return "resolve"

    @classmethod
    def allows_multiple(cls) -> bool:
        return True


class NodeCreator(NodeTransformLifecycle, abc.ABC):
    """Abstract class for nodes that "expand" functions into other nodes."""

    @abc.abstractmethod
    def generate_node(self, fn: Callable, config: Dict[str, Any]) -> node.Node:
        """Given a function, converts it to a series of nodes that it produces.

        :param config:
        :param fn: A function to convert.
        :return: A collection of nodes.
        """
        pass

    @abc.abstractmethod
    def validate(self, fn: Callable):
        """Validates that a function will work with this expander

        :param fn: Function to validate.
        :raises InvalidDecoratorException if this is not a valid function for the annotator
        """
        pass

    @classmethod
    def get_lifecycle_name(cls) -> str:
        return "generate"

    @classmethod
    def allows_multiple(cls) -> bool:
        return False


class SubDAGModifier(NodeTransformLifecycle, abc.ABC):
    @abc.abstractmethod
    def transform_dag(
        self, nodes: Collection[node.Node], config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """Modifies a DAG consisting of a set of nodes. Note that this is to support the following two base classes.

        :param nodes: Collection of nodes (not necessarily connected) to modify
        :param config: Configuration in case any is needed
        :return: the new DAG of nodes
        """
        pass


class NodeExpander(SubDAGModifier):
    """Expands a node into multiple nodes. This is a special case of the SubDAGModifier,
    which allows modification of some portion of the DAG. This just modifies a single node."""

    EXPAND_NODES = "expand_nodes"

    def transform_dag(
        self, nodes: Collection[node.Node], config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        if len(nodes) != 1:
            raise ValueError(
                f"Cannot call NodeExpander: {self.__class__} on more than one node. This must be called first in the DAG. Called with {nodes}"
            )
        (node_,) = nodes
        return self.expand_node(node_, config, fn)

    @abc.abstractmethod
    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """Given a single node, expands into multiple nodes. Note that this node list includes:
        1. Each "output" node (think sink in a DAG)
        2. All intermediate steps
        So in essence, this forms a miniature DAG

        :param node: The node to expand
        :return: A collection of nodes to add to the DAG
        """
        pass

    @abc.abstractmethod
    def validate(self, fn: Callable):
        pass

    @classmethod
    def get_lifecycle_name(cls) -> str:
        return "expand"

    @classmethod
    def allows_multiple(cls) -> bool:
        return False


class NodeTransformer(SubDAGModifier):
    NON_FINAL_TAG = "hamilton.decorators.non_final"  # TODO -- utilize this in _separate_final_nodes

    @staticmethod
    def _separate_final_nodes(
        nodes: Collection[node.Node],
    ) -> Tuple[Collection[node.Node], Collection[node.Node]]:
        """Separates out final nodes (sinks) from the nodes.

        :param nodes: Nodes to separate out
        :return: A tuple consisting of [internal, final] node sets
        """

        def node_tagged_non_final(node_: node.Node):
            return node_.tags.get(NodeTransformer.NON_FINAL_TAG, False)  # Defaults to final

        non_final_nodes = set()
        for node_ in nodes:
            for dep in node_.input_types:
                if not node_tagged_non_final(node_):
                    non_final_nodes.add(dep)
        return [node_ for node_ in nodes if node_.name in non_final_nodes], [
            node_ for node_ in nodes if node_.name not in non_final_nodes
        ]

    def transform_dag(
        self, nodes: Collection[node.Node], config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """Finds the sources and sinks and runs the transformer on each sink.
        Then returns the result of the entire set of sinks. Note that each sink has to have a unique name.

        :param config: The original function we're messing with
        :param nodes: Subdag to modify
        :param fn: Original function that we're utilizing/modifying
        :return: The DAG of nodes in this node
        """
        internal_nodes, final_nodes = self._separate_final_nodes(nodes)
        out = list(internal_nodes)
        for sink in final_nodes:
            out += list(self.transform_node(sink, config, fn))
        return out

    @abc.abstractmethod
    def transform_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        pass

    @abc.abstractmethod
    def validate(self, fn: Callable):
        pass

    @classmethod
    def get_lifecycle_name(cls) -> str:
        return "transform"

    @classmethod
    def allows_multiple(cls) -> bool:
        return True


class NodeDecorator(NodeTransformer, abc.ABC):
    DECORATE_NODES = "decorate_nodes"

    def transform_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """Transforms the node. Delegates to decorate_node

        :param node_: Node to transform
        :param config: Config in case its needed
        :param fn: Function we're decorating
        :return: The nodes produced by the transformation
        """
        return [self.decorate_node(node_)]

    @classmethod
    def get_lifecycle_name(cls) -> str:
        return NodeDecorator.DECORATE_NODES

    @classmethod
    def allows_multiple(cls) -> bool:
        return True

    def validate(self, fn: Callable):
        pass

    @abc.abstractmethod
    def decorate_node(self, node_: node.Node) -> node.Node:
        """Decorates the node -- copies and embellishes in some way.

        :param node_: Node to decorate.
        :return: A copy of the node.
        """
        pass


class DefaultNodeCreator(NodeCreator):
    def generate_node(self, fn: Callable, config: Dict[str, Any]) -> node.Node:
        return node.Node.from_fn(fn)

    def validate(self, fn: Callable):
        pass


class DefaultNodeResolver(NodeResolver):
    def resolve(self, fn: Callable, configuration: Dict[str, Any]) -> Callable:
        return fn

    def validate(self, fn):
        pass


class DefaultNodeExpander(NodeExpander):
    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        return [node_]

    def validate(self, fn: Callable):
        pass


class DefaultNodeDecorator(NodeDecorator):
    def decorate_node(self, node_: node.Node) -> node.Node:
        return node_


def resolve_nodes(fn: Callable, config: Dict[str, Any]) -> Collection[node.Node]:
    """Gets a list of nodes from a function. This is meant to be an abstraction between the node
    and the function that it implements. This will end up coordinating with the decorators we build
    to modify nodes.

    Algorithm is as follows:
    1. If there is a list of function resolvers, apply them one
    after the other. Otherwise, apply the default function resolver
    which will always return just the function. This determines whether to
    proceed -- if any function resolver is none, short circuit and return
    an empty list of nodes.

    2. If there is a list of node creators, that list must be of length 1
    -- this is determined in the node creator class. Apply that to get
    the initial node.

    3. If there is a list of node expanders, apply them. Otherwise apply the default
    nodeexpander This must be a
    list of length one. This gives out a list of nodes.

    4. If there is a node transformer, apply that. Note that the node transformer
    gets applied individually to just the sink nodes in the subdag. It subclasses
    "DagTransformer" to do so.

    5. Return the final list of nodes.

    :param fn: Function to input.
    :return: A list of nodes into which this function transforms.
    """
    node_resolvers = getattr(fn, NodeResolver.get_lifecycle_name(), [DefaultNodeResolver()])
    for resolver in node_resolvers:
        fn = resolver.resolve(fn, config)
        if fn is None:
            return []
    (node_creator,) = getattr(fn, NodeCreator.get_lifecycle_name(), [DefaultNodeCreator()])
    nodes = [node_creator.generate_node(fn, config)]
    (node_expander,) = getattr(fn, NodeExpander.get_lifecycle_name(), [DefaultNodeExpander()])
    nodes = node_expander.transform_dag(nodes, config, fn)
    node_transformers = getattr(fn, NodeTransformer.get_lifecycle_name(), [])
    for dag_modifier in node_transformers:
        nodes = dag_modifier.transform_dag(nodes, config, fn)
    node_decorators = getattr(fn, NodeDecorator.get_lifecycle_name(), [DefaultNodeDecorator()])
    for node_decorator in node_decorators:
        nodes = node_decorator.transform_dag(nodes, config, fn)
    return nodes
