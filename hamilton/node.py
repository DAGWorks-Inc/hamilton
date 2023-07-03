import inspect
import typing
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

"""
Module that contains the primitive components of the graph.

These get their own file because we don't like circular dependencies.
"""


class NodeSource(Enum):
    """
    Specifies where this node's value originates.
    This can be used by different adapters to flexibly execute a function graph.
    """

    STANDARD = 1  # standard dependencies
    EXTERNAL = 2  # This node's value should be taken from cache
    PRIOR_RUN = 3  # This node's value should be taken from a prior run.
    # This is not used in a standard function graph, but it comes in handy for repeatedly running the same one.


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

    def __init__(
        self,
        name: str,
        typ: Type,
        doc_string: str = "",
        callabl: Callable = None,
        node_source: NodeSource = NodeSource.STANDARD,
        input_types: Dict[str, Union[Type, Tuple[Type, DependencyType]]] = None,
        tags: Dict[str, Any] = None,
        namespace: Tuple[str, ...] = (),
        originating_functions: Optional[Tuple[Callable, ...]] = None,
    ):
        """Constructor for our Node object.

        :param name: the name of the function.
        :param typ: the output type of the function.
        :param doc_string: the doc string for the function. Optional.
        :param callabl: the actual function callable.
        :param node_source: whether this is something someone has to pass in.
        :param input_types: the input parameters and their types.
        :param tags: the set of tags that this node contains.
        """
        if tags is None:
            tags = dict()
        self._tags = tags
        self._name = name
        self._type = typ
        if typ is None or typ == inspect._empty:
            raise ValueError(f"Missing type for hint for function {name}. Please add one to fix.")
        self._callable = callabl
        self._doc = doc_string
        self._node_source = node_source
        self._dependencies = []
        self._depended_on_by = []
        self._namespace = namespace
        self._input_types = {}
        self._originating_functions = originating_functions

        if self._node_source == NodeSource.STANDARD:
            if input_types is not None:
                for key, value in input_types.items():
                    if isinstance(value, tuple):
                        self._input_types[key] = value
                    else:
                        self._input_types = {
                            key: (value, DependencyType.REQUIRED)
                            for key, value in input_types.items()
                        }
            else:
                input_types = typing.get_type_hints(callabl)
                signature = inspect.signature(callabl)
                for key, value in signature.parameters.items():
                    if key not in input_types:
                        raise ValueError(
                            f"Missing type hint for {key} in function {name}. Please add one to fix."
                        )
                    self._input_types[key] = (
                        input_types[key],
                        DependencyType.from_parameter(value),
                    )
        elif self.user_defined:
            if input_types is not None:
                raise ValueError(
                    f"Input types cannot be provided for user-defined node {self.name}"
                )

    @property
    def namespace(self) -> Tuple[str, ...]:
        return self._namespace

    @property
    def documentation(self) -> str:
        return self._doc

    @property
    def input_types(self) -> Dict[Any, Tuple[Any, DependencyType]]:
        return self._input_types

    @property
    def name(self) -> str:
        return ".".join(self.namespace + (self._name,))

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
    def dependencies(self) -> List["Node"]:
        return self._dependencies

    @property
    def depended_on_by(self) -> List["Node"]:
        return self._depended_on_by

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @property
    def originating_functions(self) -> Optional[Tuple[Callable, ...]]:
        """Gives all functions from which this node was created. None if the data
        is not available (it is user-defined, or we have not added it yet). Note that this can be
        multiple in the case of subdags (the subdag function + the other function). In that case,
        it will be in order of creation (subdag function last).

        Note that this is filled in in function_modifiers.base -- see note in from_fn

        :return: A Tuple consisting of functions from which this node was created.
        """
        return self._originating_functions

    def add_tag(self, tag_name: str, tag_value: str):
        self._tags[tag_name] = tag_value

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f"<{self.name} {self._tags}>"

    def __eq__(self, other: "Node"):
        """Want to deeply compare nodes in a custom way.

        Current user is just unit tests. But you never know :)

        Note: we only compare names of dependencies because we don't want infinite recursion.
        """
        return (
            isinstance(other, Node)
            and self._name == other.name
            and self._type == other.type
            and self._doc == other.documentation
            and self._tags == other.tags
            and self.user_defined == other.user_defined
            and [n.name for n in self.dependencies] == [o.name for o in other.dependencies]
            and [n.name for n in self.depended_on_by] == [o.name for o in other.depended_on_by]
            and self.node_source == other.node_source
        )

    def __ne__(self, other: "Node"):
        return not self.__eq__(other)

    def __call__(self, *args, **kwargs):
        """Call just delegates to the callable, purely for clean syntactic sugar"""
        return self.callable(*args, **kwargs)

    @staticmethod
    def from_fn(fn: Callable, name: str = None) -> "Node":
        """Generates a node from a function. Optionally overrides the name.

        Note that currently, the `originating_function` is externally passed in -- this
        happens in resolve_nodes in function_modifiers.base. TBD whether we'll want it to stay there.

        :param fn: Function to generate the name from
        :param name: Name to use for the node
        :return: The node we generated
        """
        if name is None:
            name = fn.__name__
        return_type = typing.get_type_hints(fn).get("return")
        if return_type is None:
            raise ValueError(f"Missing type hint for return value in function {fn.__qualname__}.")
        module = inspect.getmodule(fn).__name__
        return Node(
            name,
            return_type,
            fn.__doc__ if fn.__doc__ else "",
            callabl=fn,
            tags={"module": module},
        )

    def copy_with(self, **overrides) -> "Node":
        """Copies a node with the specified overrides for the constructor arguments.
        Utility function for creating a node -- useful for modifying it.

        :param kwargs: kwargs to use in place of the node. Passed to the constructor.
        :return: A node copied from self with the specified keyword arguments replaced.
        """
        constructor_args = dict(
            name=self.name,
            typ=self.type,
            doc_string=self.documentation,
            callabl=self.callable,
            node_source=self.node_source,
            input_types=self.input_types,
            tags=self.tags,
            originating_functions=self.originating_functions,
        )
        constructor_args.update(**overrides)
        return Node(**constructor_args)
