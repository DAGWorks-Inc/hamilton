import abc
import dataclasses
import enum
from typing import Any

"""Utilities for specifying dependencies/dependency types in other decorators."""


class ParametrizedDependencySource(enum.Enum):
    LITERAL = "literal"
    UPSTREAM = "upstream"


class ParametrizedDependency:
    @abc.abstractmethod
    def get_dependency_type(self) -> ParametrizedDependencySource:
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


def value(literal_value: Any) -> LiteralDependency:
    """Specifies that a parameterized dependency comes from a "literal" source.
    E.G. value("foo") means that the value is actually the string value "foo"

    :param literal_value: Python literal value to use
    :return: A LiteralDependency object -- a signifier to the internal framework of the dependency type
    """
    if isinstance(literal_value, LiteralDependency):
        return literal_value
    return LiteralDependency(value=literal_value)


def source(dependency_on: Any) -> UpstreamDependency:
    """Specifies that a parameterized dependency comes from an "upstream" source.
    This means that it comes from a node somewhere else.
    E.G. source("foo") means that it should be assigned the value that "foo" outputs.

    :param dependency_on: Upstream node to come from
    :return:An UpstreamDependency object -- a signifier to the internal framework of the dependency type.
    """
    if isinstance(dependency_on, UpstreamDependency):
        return dependency_on
    return UpstreamDependency(source=dependency_on)
