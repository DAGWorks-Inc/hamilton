import abc
import collections
import functools
import logging

try:
    from types import EllipsisType
except ImportError:
    # python3.10 and above
    EllipsisType = type(...)
from typing import Any, Callable, Collection, Dict, List, Optional, Union

from hamilton import node, registry

logger = logging.getLogger(__name__)


if not registry.INITIALIZED:
    # Trigger load of extensions here because decorators are the only thing that use the registry
    # right now. Side note: ray serializes things weirdly, so we need to do this here rather than in
    # in the other choice of hamilton/base.py.
    plugins_modules = ["pandas", "polars", "pyspark_pandas", "dask", "geopandas"]
    for plugin_module in plugins_modules:
        try:
            registry.load_extension(plugin_module)
        except NotImplementedError:
            logger.debug(f"Did not load {plugin_module} extension.")
            pass
    registry.INITIALIZED = True


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


DECORATOR_COUNTER = collections.defaultdict(int)


def track_decorator_usage(call_fn: Callable) -> Callable:
    """Decorator to wrap the __call__ to count decorator usage.

    :param call_fn: the `__call__` function.
    :return: the wrapped call function.
    """

    @functools.wraps(call_fn)
    def replace__call__(self, fn):
        global DECORATOR_COUNTER
        if self.__module__.startswith("hamilton.function_modifiers"):
            # only capture counts for hamilton decorators
            DECORATOR_COUNTER[self.__class__.__name__] = (
                DECORATOR_COUNTER[self.__class__.__name__] + 1
            )
        else:
            DECORATOR_COUNTER["custom_decorator"] = DECORATOR_COUNTER["custom_decorator"] + 1
        return call_fn(self, fn)

    return replace__call__


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

    @track_decorator_usage
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

    def required_config(self) -> Optional[List[str]]:
        """Declares the required configuration keys for this decorator.
        Note that these configuration keys will be filtered and passed to the `configuration`
        parameter of the functions that this decorator uses.

        Note that this currently allows for a "escape hatch".
        That is, returning None from this function.

        :return: A list of the required configuration keys.
        """
        return []

    def optional_config(self) -> Dict[str, Any]:
        """Declares the optional configuration keys for this decorator.
        These are configuration keys that can be used by the decorator, but are not required.
        Along with these we have *defaults*, which we will use to pass to the config.

        :return:
        """
        return {}

    @property
    def name(self) -> str:
        """Name of the decorator.

        :return: The name of the decorator
        """
        return self.__class__.__name__


class NodeResolver(NodeTransformLifecycle):
    """Decorator to resolve a nodes function. Can modify anything about the function and is run at DAG creation time."""

    @abc.abstractmethod
    def resolve(self, fn: Callable, config: Dict[str, Any]) -> Optional[Callable]:
        """Determines what a function resolves to. Returns None if it should not be included in the DAG.

        :param fn: Function to resolve
        :param config: DAG config
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
    def generate_nodes(self, fn: Callable, config: Dict[str, Any]) -> List[node.Node]:
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


TargetType = Union[str, Collection[str], None, EllipsisType]


class NodeTransformer(SubDAGModifier):
    def __init__(self, target: TargetType):
        """Target determines to which node(s) this applies. This represents selection from a subDAG.
        For the options, consider at the following graph:
        A -> B -> C
             \_> D -> E

        1. If it is `None`, it defaults to the "old" behavior. That is, is applies to all "final" DAG
        nodes. In the subdag. That is, all nodes with out-degree zero/sinks. In the case
        above, *just* C and E will be transformed.

        2. If it is a string, it will be interpreted as a node name. In the above case, if it is A, it
        will transform A, B will transform B, etc...

        3. If it is a collection of strings, it will be interpreted as a collection of node names.
        That is, it will apply to all nodes that are referenced in that collection. In the above case,
        if it is ["A", "B"], it will transform to A and B.

        4. If it is Ellipsis, it will apply to all nodes in the subDAG. In the above case, it will
        transform A, B, C, D, and E.

        :param target: Which node(s)/node spec to run transforms on top of. These nodes will get
        replaced by a list of nodes.
        """
        self.target = target

    @staticmethod
    def _extract_final_nodes(
        nodes: Collection[node.Node],
    ) -> Collection[node.Node]:
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
        return [node_ for node_ in nodes if node_.name not in non_final_nodes]

    @staticmethod
    def select_nodes(target: TargetType, nodes: Collection[node.Node]) -> Collection[node.Node]:
        """Resolves all nodes to match the target. This does a resolution on the rules
        specified in the constructor above, giving a set of nodes that match a target.
        We then can split them from the remainder of nodes, and just transform them.

        :param target: The target to use to resolve nodes
        :param nodes: SubDAG to resolve.
        :return: The set of nodes matching this target
        """
        if target is None:
            return NodeTransformer._extract_final_nodes(nodes)
        elif target is Ellipsis:
            return nodes
        elif isinstance(target, str):
            out = [node_ for node_ in nodes if node_.name == target]
            if len(out) == 0:
                raise InvalidDecoratorException(f"Could not find node {target} in {nodes}")
            return out
        elif isinstance(target, Collection):
            out = [node_ for node_ in nodes if node_.name in target]
            if len(out) != len(target):
                raise InvalidDecoratorException(
                    f"Could not find all nodes {target} in {nodes}. "
                    f"Missing ({set(target) - set([node_.name for node_ in out])})"
                )
            return out
        else:
            raise ValueError(f"Invalid target: {target}")

    @staticmethod
    def compliment(
        all_nodes: Collection[node.Node], nodes_to_transform: Collection[node.Node]
    ) -> Collection[node.Node]:
        """Given a set of nodes, and a set of nodes to transform, returns the set of nodes that
        are not in the set of nodes to transform.

        :param all_nodes: All nodes in the subdag
        :param nodes_to_transform: All nodes to transform
        :return: A collection of nodes that are not in the set of nodes to transform but are in the
        subdag
        """
        return [node_ for node_ in all_nodes if node_ not in nodes_to_transform]

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
        nodes_to_transform = self.select_nodes(self.target, nodes)
        nodes_to_keep = self.compliment(nodes, nodes_to_transform)
        out = list(nodes_to_keep)
        for node_to_transform in nodes_to_transform:
            out += list(self.transform_node(node_to_transform, config, fn))
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

    def __init__(self, target: TargetType):
        """Initializes a NodeDecorator with a target, to determine *which* nodes to decorate.
        See documentation in NodeTransformer for more details on what to decorate.

        :param target: Target parameter to resolve set of nodes to transform.
        """
        super().__init__(target=target)

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
    def generate_nodes(self, fn: Callable, config: Dict[str, Any]) -> List[node.Node]:
        return [node.Node.from_fn(fn)]

    def validate(self, fn: Callable):
        pass


class DefaultNodeResolver(NodeResolver):
    def resolve(self, fn: Callable, config: Dict[str, Any]) -> Callable:
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
    def __init__(self):
        super().__init__(target=...)

    def decorate_node(self, node_: node.Node) -> node.Node:
        return node_


def resolve_config(
    name_for_error: str,
    config: Dict[str, Any],
    config_required: List[str],
    config_optional_with_defaults: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolves the configuration that a decorator utilizes

    :param name_for_error:
    :param config:
    :param config_required:
    :param config_optional_with_defaults:
    :return:
    """
    missing_keys = (
        set(config_required) - set(config.keys()) - set(config_optional_with_defaults.keys())
    )
    if len(missing_keys) > 0:
        raise MissingConfigParametersException(
            f"The following configurations are required by {name_for_error}: {missing_keys}"
        )
    config_out = {key: config[key] for key in config_required}
    for key in config_optional_with_defaults:
        config_out[key] = config.get(key, config_optional_with_defaults[key])
    return config_out


def filter_config(config: Dict[str, Any], decorator: NodeTransformLifecycle) -> Dict[str, Any]:
    """Filters the config to only include the keys in config_required
    TODO -- break this into two so we can make it easier to test.

    :param config: The config to filter
    :param config_required: The keys to include
    :param decorator: The decorator that is utilizing the configuration
    :return: The filtered config
    """
    config_required = decorator.required_config()
    config_optional_with_defaults = decorator.optional_config()
    if config_required is None:
        # This is an out to allow for backwards compatibility for the config.resolve decorator
        # Note this is an internal API, but we made the config with the `resolve` parameter public
        return config
    return resolve_config(decorator.name, config, config_required, config_optional_with_defaults)


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
    :param config: Configuratino to use -- this can be used by decorators to specify
    which configuration they need.
    :return: A list of nodes into which this function transforms.
    """
    node_resolvers = getattr(fn, NodeResolver.get_lifecycle_name(), [DefaultNodeResolver()])
    for resolver in node_resolvers:
        fn = resolver.resolve(fn, config=filter_config(config, resolver))
        if fn is None:
            return []
    (node_creator,) = getattr(fn, NodeCreator.get_lifecycle_name(), [DefaultNodeCreator()])
    nodes = node_creator.generate_nodes(fn, filter_config(config, node_creator))
    if hasattr(fn, NodeExpander.get_lifecycle_name()):
        (node_expander,) = getattr(fn, NodeExpander.get_lifecycle_name(), [DefaultNodeExpander()])
        nodes = node_expander.transform_dag(nodes, filter_config(config, node_expander), fn)
    node_transformers = getattr(fn, NodeTransformer.get_lifecycle_name(), [])
    for dag_modifier in node_transformers:
        nodes = dag_modifier.transform_dag(nodes, filter_config(config, dag_modifier), fn)
    node_decorators = getattr(fn, NodeDecorator.get_lifecycle_name(), [DefaultNodeDecorator()])
    for node_decorator in node_decorators:
        nodes = node_decorator.transform_dag(nodes, filter_config(config, node_decorator), fn)
    return nodes


class InvalidDecoratorException(Exception):
    pass


class MissingConfigParametersException(Exception):
    pass
