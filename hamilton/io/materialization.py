import typing

from hamilton import node
from hamilton.base import ResultMixin
from hamilton.function_modifiers import dependencies


class SaveTo:
    def __init__(self, *columns: str):
        self.columns = columns

    def to_nodes(self) -> typing.List[node.Node]:
        raise NotImplementedError()


class SaveToCSV:
    def __init__(
        self,
        pathname: typing.Union[dependencies.LiteralDependency, dependencies.UpstreamDependency],
        results_builder: ResultMixin,
        *columns: str
    ):
        self.pathname = pathname
        self.results_builder = results_builder
