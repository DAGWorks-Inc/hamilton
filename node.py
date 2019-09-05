import inspect
import logging
from typing import Type, Dict, Any, Callable, List


logger = logging.getLogger(__name__)

"""
Module that contains the primitive components of the graph.

These get their own file because we don't like circular dependencies.
"""


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

    @staticmethod
    def from_fn(fn: Callable, name: str=None) -> 'Node':
        """Generates a node from a function. Optionally overrides the name.

        :param fn: Function to generate the name from
        :param name: Name to use for the node
        :return: The node we generated
        """
        if name is None:
            name = fn.__name__
        sig = inspect.signature(fn)
        return Node(name, sig.return_annotation, fn.__doc__ if fn.__doc__ else '', callabl=fn)
