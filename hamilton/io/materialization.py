import sys
import typing
from typing import Any, Dict, List, Optional, Type, Union

from hamilton import base, graph, node
from hamilton.function_modifiers.adapters import SaveToDecorator
from hamilton.function_modifiers.dependencies import SingleDependency, value
from hamilton.graph import FunctionGraph
from hamilton.io.data_adapters import DataSaver
from hamilton.registry import SAVER_REGISTRY


class materialization_meta__(type):
    """Metaclass for the load_from decorator. This is specifically to allow class access method.
    Note that there *is* another way to do this -- we couold add attributes dynamically on the
    class in registry, or make it a function that just proxies to the decorator. We can always
    change this up, but this felt like a pretty clean way of doing it, where we can decouple the
    registry from the decorator class.
    """

    def __getattr__(cls, item: str):
        if item in SAVER_REGISTRY:
            potential_loaders = SAVER_REGISTRY[item]
            savers = [loader for loader in potential_loaders if issubclass(loader, DataSaver)]
            if len(savers) > 0:
                return Materialize.partial(SAVER_REGISTRY[item])
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(
                f"No data materializer named: {item}. "
                f"Available materializers are: {SAVER_REGISTRY.keys()}. "
                "If you've gotten to this point, you either (1) spelled the "
                "loader name wrong, (2) are trying to use a loader that does"
                "not exist (yet). For a list of available materializers, see  "
                "https://hamilton.readthedocs.io/reference/io/available-data-adapters/#data"
                "-loaders "
            ) from e


class MaterializerFactory:
    """Basic factory for creating materializers. Note that this should only ever be instantiated
    through `to.<name>`, which conducts polymorphic lookup to find the appropriate materializer."""

    def __init__(
        self,
        id: str,
        savers: List[Type[DataSaver]],
        result_builder: Optional[base.ResultMixin],
        dependencies: List[str],
        **data_saver_kwargs: Any,
    ):
        """Creates a materializer factory.

        :param name: Name of the node that this will represent in the DAG.
        :param savers: Potential data savers to use (these will be filtered down from the ones
        registered to <output_format> when `to.<output_format>` is called.)
        :param result_builder: ResultBuilder that joins the result of the dependencies together.
        :param dependencies: Nodes, the results of which on which this depends
        :param data_saver_kwargs: kwargs to be passed to the data_savers. Either literal values,
        or `source`/`value`.
        """

        self.id = id
        self.savers = savers
        self.result_builder = result_builder
        self.dependencies = dependencies
        self.data_saver_kwargs = self._process_kwargs(data_saver_kwargs)

    @staticmethod
    def _process_kwargs(
        data_saver_kwargs: Dict[str, Union[Any, SingleDependency]]
    ) -> Dict[str, SingleDependency]:
        """Processes raw strings from the user, converting them into dependency specs.

        :param data_saver_kwargs: Kwargs passed in from the user
        :return:
        """
        processed_kwargs = {}
        for kwarg, kwarg_val in data_saver_kwargs.items():

            if not isinstance(kwarg_val, SingleDependency):
                processed_kwargs[kwarg] = value(kwarg_val)
            else:
                processed_kwargs[kwarg] = kwarg_val
        return processed_kwargs

    def _resolve_dependencies(self, fn_graph: graph.FunctionGraph) -> List[node.Node]:
        return [fn_graph.nodes[name] for name in self.dependencies]

    def resolve(self, fn_graph: graph.FunctionGraph) -> List[node.Node]:
        """Resolves a materializer, returning the set of nodes that should get
        appended to the function graph. This does two things:

        1. Adds a node that handles result-building
        2. Adds a node that handles data-saving, reusing the data saver functionality.

        :param graph: Function Graph to which we are adding the materializer
        :return: List of nodes to add to the graph
        """
        node_dependencies = self._resolve_dependencies(fn_graph)

        def join_function(**kwargs):
            return self.result_builder.build_result(**kwargs)

        out = []
        if self.result_builder is None:
            if len(node_dependencies) != 1:
                raise ValueError(
                    "Must specify result builder if the materializer has more than one dependency "
                    "it is materializing. Otherwise we have no way to join them before storage! "
                    f"See materializer {self.id}."
                )
            save_dep = node_dependencies[0]
        else:
            join_node = node.Node(
                name=f"{self.id}_build_result",
                typ=self.result_builder.output_type(),
                doc_string=f"Builds the result for {self.id} materializer",
                callabl=join_function,
                input_types={dep.name: dep.type for dep in node_dependencies},
                originating_functions=None
                if self.result_builder is None
                else [self.result_builder.build_result],
            )
            out.append(join_node)
            save_dep = join_node

        out.append(
            # We can reuse the functionality in the save_to decorator
            SaveToDecorator(self.savers, self.id, **self.data_saver_kwargs).create_saver_node(
                save_dep, {}, save_dep.callable
            )
        )
        return out


# TODO -- get rid of this once we decide to no longer support 3.7
if sys.version_info >= (3, 8):
    from typing import Protocol

    @typing.runtime_checkable
    class _FactoryProtocol(Protocol):
        """Typing for the create_materializer_factory function"""

        def __call__(
            self,
            id: str,
            dependencies: List[str],
            combine: base.ResultMixin = None,
            **kwargs: Union[str, SingleDependency],
        ) -> MaterializerFactory:
            ...

else:
    _FactoryProtocol = object


class Materialize(metaclass=materialization_meta__):
    """Materialize class to facilitate easy reference. Note that you should never need to refer
    to this directly. Rather, this should be referred as `to` in hamilton.io."""

    @classmethod
    def partial(cls, data_savers: List[Type[DataSaver]]) -> _FactoryProtocol:
        """Creates a partial materializer, with the specified data savers."""

        def create_materializer_factory(
            id: str,
            dependencies: List[str],
            combine: base.ResultMixin = None,
            **kwargs: typing.Any,
        ) -> MaterializerFactory:
            return MaterializerFactory(
                id=id,
                savers=data_savers,
                result_builder=combine,
                dependencies=dependencies,
                **kwargs,
            )

        return create_materializer_factory


# Note that we have a circular import issue that we'll need to deal with
# This refers to save_to, which also refers to the io module
# Its fine for now, as we can just import this directly and avoid calling it in
# the __init__.py of io, but we'll likely want to think this through and extract the tooling
# in save_to to a separate, third module.
class to(Materialize):
    """This is the entry point for Materialization. Note that this is coupled
    with the driver's materialize function -- properties are dynamically assigned
    based on data savers that have been registered, allowing you to call `to.csv`/`to.json`.
    For full documentation, see the documentation for the `materialize` function in the hamilton
    driver."""

    pass


def modify_graph(
    fn_graph: FunctionGraph, materializer_factories: List[MaterializerFactory]
) -> FunctionGraph:
    """Modifies the function graph, adding in the specified materialization nodes.
    This is purely a utility function to make reading upstream code easier.

    :param graph: Graph to modify.
    :param materializers: Materializers to add to the graph
    :return: A new graph with the materializers.
    """
    additional_nodes = []
    for materializer in materializer_factories:
        additional_nodes.extend(materializer.resolve(fn_graph))
    return fn_graph.with_nodes({node_.name: node_ for node_ in additional_nodes})
