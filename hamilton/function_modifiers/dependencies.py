import abc
import dataclasses
import enum
import typing
from typing import Any, List, Mapping, Sequence, Type

import typing_inspect

from hamilton.function_modifiers.base import InvalidDecoratorException

"""Utilities for specifying dependencies/dependency types in other decorators."""


class ParametrizedDependencySource(enum.Enum):
    LITERAL = "literal"
    UPSTREAM = "upstream"
    GROUPED_LIST = "grouped_list"
    GROUPED_DICT = "grouped_dict"


class ParametrizedDependency:
    @abc.abstractmethod
    def get_dependency_type(self) -> ParametrizedDependencySource:
        pass


class SingleDependency(ParametrizedDependency, abc.ABC):
    pass


@dataclasses.dataclass
class LiteralDependency(ParametrizedDependency):
    value: Any

    def get_dependency_type(self) -> ParametrizedDependencySource:
        return ParametrizedDependencySource.LITERAL


@dataclasses.dataclass
class UpstreamDependency(ParametrizedDependency):
    source: str

    def get_dependency_type(self) -> ParametrizedDependencySource:
        return ParametrizedDependencySource.UPSTREAM


class GroupedDependency(ParametrizedDependency, abc.ABC):
    @classmethod
    @abc.abstractmethod
    def resolve_dependency_type(cls, annotated_type: Type[Type], param_name: str) -> Type[Type]:
        """Resolves dependency type for an annotated parameter. E.G. List[str] -> str,
        or Dict[str, int] -> int.

        :param type: Type to inspect
        :param param_name: Name of the parameter, used for good error messages
        :return:Resolved dependency type
        :raises: InvalidDecoratorException if the dependency type cannot be resolved appropriately.
        """


@dataclasses.dataclass
class GroupedListDependency(ParametrizedDependency):
    sources: List[ParametrizedDependency]

    @classmethod
    def resolve_dependency_type(cls, annotated_type: Type[Sequence[Type]], param_name: str):
        origin = typing_inspect.get_origin(annotated_type)
        if origin is None or not issubclass(origin, typing.Sequence):
            raise InvalidDecoratorException(
                f"Type: {annotated_type} for parameter: {param_name} needs to be "
                f"sequence to use the group() dependency specification. Otherwise hamilton"
                f"cannot validate that the types are correct."
            )
        args = typing_inspect.get_args(annotated_type)
        if not len(args) == 1:
            raise InvalidDecoratorException(
                f"Type: {annotated_type} for parameter: {param_name} needs to be "
                f"sequence with one type argument to use the group() dependency specification. "
                f"Otherwise Hamilton cannot validate that the types are correct."
            )
        return args[0]

    def get_dependency_type(self) -> ParametrizedDependencySource:
        return ParametrizedDependencySource.GROUPED_LIST


@dataclasses.dataclass
class GroupedDictDependency(ParametrizedDependency):
    sources: typing.Dict[str, ParametrizedDependency]

    def get_dependency_type(self) -> ParametrizedDependencySource:
        return ParametrizedDependencySource.GROUPED_DICT

    @classmethod
    def resolve_dependency_type(cls, annotated_type: Type[Mapping[str, Type]], param_name: str):
        origin = typing_inspect.get_origin(annotated_type)
        if origin is None or not issubclass(origin, typing.Mapping):
            raise InvalidDecoratorException(
                f"Type: {annotated_type} for parameter: {param_name} needs to be a"
                f"mapping type to use the group() dependency specification with **kwargs. "
                f"Otherwise hamilton cannot validate that the types are correct!"
            )
        args = typing_inspect.get_args(annotated_type)
        if not len(args) == 2 or not issubclass(args[0], str):
            raise InvalidDecoratorException(
                f"Type: {annotated_type} for parameter: {param_name} needs to be a"
                f"mapping with types [str, Type] to use the group() dependency specification with "
                f"**kwargs. Otherwise Hamilton cannot validate that the types are correct."
            )
        return args[1]


def value(literal_value: Any) -> LiteralDependency:
    """Specifies that a parameterized dependency comes from a "literal" source.

    E.G. value("foo") means that the value is actually the string value "foo".

    :param literal_value: Python literal value to use.
    :return: A LiteralDependency object -- a signifier to the internal framework of the dependency type.
    """
    if isinstance(literal_value, LiteralDependency):
        return literal_value
    return LiteralDependency(value=literal_value)


def source(dependency_on: Any) -> UpstreamDependency:
    """Specifies that a parameterized dependency comes from an `upstream` source.

    This means that it comes from a node somewhere else. E.G. source("foo") means that it should be assigned the \
    value that "foo" outputs.

    :param dependency_on: Upstream function (i.e. node) to come from.
    :return: An UpstreamDependency object -- a signifier to the internal framework of the dependency type.
    """
    if isinstance(dependency_on, UpstreamDependency):
        return dependency_on
    return UpstreamDependency(source=dependency_on)


def group(
    *dependency_args: ParametrizedDependency, **dependency_kwargs: ParametrizedDependency
) -> GroupedListDependency:
    """Specifies that a parameterized dependency comes from a "grouped" source.

    This means that it gets injected into a list of dependencies that are grouped together. E.G.
    dep=group(source("foo"), source("bar")) for the function:

    def f(dep: List[pd.Series]) -> pd.Series:
        return ...

    Would result in dep getting foo and bar dependencies injected.
    :param dependencies: Dependencies, list of dependencies
    :return:
    """
    if dependency_args and dependency_kwargs:
        raise ValueError(
            "group() can either represent a dictionary or a list of dpeendencies, " "not both!"
        )
    if dependency_args:
        return GroupedListDependency(sources=list(dependency_args))
    return GroupedDictDependency(sources=dependency_kwargs)
