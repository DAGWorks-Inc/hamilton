import abc
import dataclasses
import enum
import typing
from typing import Any, List, Sequence, Type

import typing_inspect

"""Utilities for specifying dependencies/dependency types in other decorators."""


class ParametrizedDependencySource(enum.Enum):
    LITERAL = "literal"
    UPSTREAM = "upstream"
    GROUPED_LIST = "grouped_list"


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


@dataclasses.dataclass
class GroupedListDependency(ParametrizedDependency):
    @staticmethod
    def get_type(containing_type: Type[Sequence[Type]], param_name: str):
        origin = typing_inspect.get_origin(containing_type)
        if origin is None or not issubclass(origin, typing.Sequence):
            raise ValueError(
                f"Type: {containing_type} for parameter: {param_name} needs to be "
                f"sequence to use the group() dependency specification. Otherwise hamilton"
                f"cannot validate that the types are correct."
            )
        args = typing_inspect.get_args(containing_type)
        if not len(args) == 1:
            raise ValueError(
                f"Type: {containing_type} for parameter: {param_name} needs to be "
                f"sequence with one type argument to use the group() dependency specification. "
                f"Otherwise Hamilton cannot validate that the types are correct."
            )
        return args[0]

    def get_dependency_type(self) -> ParametrizedDependencySource:
        return ParametrizedDependencySource.GROUPED_LIST

    sources: List[ParametrizedDependency]


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


def group(*dependencies: ParametrizedDependency) -> GroupedListDependency:
    """Specifies that a parameterized dependency comes from a "grouped" source.

    This means that it gets injected into a list of dependencies that are grouped together. E.G.
    dep=group(source("foo"), source("bar")) for the function:

    def f(dep: List[pd.Series]) -> pd.Series:
        return ...

    Would result in dep getting foo and bar dependencies injected.
    :param dependencies: Dependencies, list of dependencies
    :return:
    """
    return GroupedListDependency(sources=list(dependencies))
