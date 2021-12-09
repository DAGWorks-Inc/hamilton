import copy
import inspect
import logging
from enum import Enum
from typing import Type, Dict, Any, Callable, List, Tuple, Union

logger = logging.getLogger(__name__)

"""
Module that contains the primitive components of the graph.

These get their own file because we don't like circular dependencies.
"""


class NodeSource(Enum):
    """Specifies where this node's value originates. This can be used by different executors to flexibly execute a function graph."""
    STANDARD = 1  # standard dependencies
    EXTERNAL = 2  # This node's value should be taken from cache
    PRIOR_RUN = 3  # This node's value sould be taken from a prior run. This is not used in a standard function graph, but it comes in handy for repeatedly running the same one.


class DependencyType(Enum):
    REQUIRED = 1
    OPTIONAL = 2

    @staticmethod
    def from_parameter(param: inspect.Parameter):
        if param.default == inspect.Parameter.empty:
            return DependencyType.REQUIRED
        return DependencyType.OPTIONAL


class Node(object):
    """Object representing a node of computation."""

    def __init__(self,
                 name: str,
                 typ: Type,
                 doc_string: str = '',
                 callabl: Callable = None,
                 node_source: NodeSource = NodeSource.STANDARD,
                 input_types: Dict[str, Union[Type, Tuple[Type, DependencyType]]] = None):
        """Constructor for our Node object.

        :param name: the name of the function.
        :param typ: the output type of the function.
        :param doc_string: the doc string for the function. Optional.
        :param callabl: the actual function callable.
        :param node_source: whether this is something someone has to pass in.
        :param input_types: the input parameters and their types.
        """
        self._name = name
        self._type = typ
        if typ is None or typ == inspect._empty:
            raise ValueError(f'Missing type for hint for function {name}. Please add one to fix.')
        self._callable = callabl
        self._doc = doc_string
        self._node_source = node_source
        self._dependencies = []
        self._depended_on_by = []

        if self._node_source == NodeSource.STANDARD:
            if input_types is not None:
                self._input_types = {}
                for key, value in input_types.items():
                    if isinstance(value, tuple):
                        self._input_types[key] = value
                    else:
                        self._input_types = {key: (value, DependencyType.REQUIRED) for key, value in input_types.items()}
            else:
                signature = inspect.signature(callabl)
                self._input_types = {}
                for key, value in signature.parameters.items():
                    if value.annotation == inspect._empty:
                        raise ValueError(f'Missing type hint for {key} in function {name}. Please add one to fix.')
                    self._input_types[key] = (value.annotation, DependencyType.from_parameter(value))

    @property
    def documentation(self) -> str:
        return self._doc

    @property
    def input_types(self) -> Dict[Any, Tuple[Any, DependencyType]]:
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

    # TODO - deprecate in favor of the node sources above
    @property
    def user_defined(self):
        return self._node_source == NodeSource.EXTERNAL

    @property
    def node_source(self):
        return self._node_source

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
        return (isinstance(other, Node) and
                self._name == other.name and
                self._type == other.type and
                self._doc == other.documentation and
                self.user_defined == other.user_defined and
                [n.name for n in self.dependencies] == [o.name for o in other.dependencies] and
                [n.name for n in self.depended_on_by] == [o.name for o in other.depended_on_by] and
                self.node_source == other.node_source)

    def __ne__(self, other: 'Node'):
        return not self.__eq__(other)

    @staticmethod
    def from_fn(fn: Callable, name: str = None) -> 'Node':
        """Generates a node from a function. Optionally overrides the name.

        :param fn: Function to generate the name from
        :param name: Name to use for the node
        :return: The node we generated
        """
        if name is None:
            name = fn.__name__
        sig = inspect.signature(fn)
        return Node(name, sig.return_annotation, fn.__doc__ if fn.__doc__ else '', callabl=fn)
