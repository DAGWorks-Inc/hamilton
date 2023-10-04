import dataclasses
import functools
import inspect
import sys
import typing
from typing import Any, Dict, List, Optional, Set, Type, Union

from hamilton import base, common, graph, node
from hamilton.function_modifiers.adapters import SaveToDecorator
from hamilton.function_modifiers.dependencies import SingleDependency, value
from hamilton.graph import FunctionGraph
from hamilton.io.data_adapters import DataSaver
from hamilton.registry import SAVER_REGISTRY


class materialization_meta__(type):
    """Metaclass for the load_from decorator. This is specifically to allow class access method.
    This only exists to add more helpful error messages. We dynamically assign the attributes
    below (see `_set_materializer_attrs`), which helps with auto-complete.
    """

    def __new__(cls, name, bases, clsdict):
        """Boiler plate for a metaclass -- this just instantiates it as a type. It also sets the
        annotations to be available later.
        """
        clsobj = super().__new__(cls, name, bases, clsdict)
        clsobj.__annotations__ = {}
        return clsobj

    def __getattr__(cls, item: str) -> "MaterializerFactory":
        """This *just* exists to provide a more helpful error message. If you try to access
        a property that doesn't exist, we'll raise an error that tells you what properties
        are available/where to learn more."""
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
        dependencies: List[Union[str, Any]],
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

    def sanitize_dependencies(self, module_set: Set[str]) -> "MaterializerFactory":
        """Sanitizes the dependencies to ensure they're strings.

        This replaces the internal value for self.dependencies and returns a new object.
        We return a new object to not modify the one passed in.

        :param module_set: modules that "functions" could come from if that's passed in.
        :return: new object with sanitized_dependencies.
        """
        final_vars = common.convert_output_values(self.dependencies, module_set)
        return MaterializerFactory(
            self.id, self.savers, self.result_builder, final_vars, **self.data_saver_kwargs
        )

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
                    "Must specify result builder via combine= key word argument if the materializer has more than "
                    "one dependency it is materializing. Otherwise we have no way to join them and know what to pass! "
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


def partial_materializer(data_savers: List[Type[DataSaver]]) -> _FactoryProtocol:
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


class Materialize(metaclass=materialization_meta__):
    """Materialize class to facilitate easy reference. Note that you should never need to refer
    to this directly. Rather, this should be referred as `to` in hamilton.io."""


class to(Materialize):
    """This is the entry point for Materialization. Note that this is coupled
    with the driver's materialize function -- properties are dynamically assigned
    based on data savers that have been registered, allowing you to call `to.csv`/`to.json`.
    For full documentation, see the documentation for the `materialize` function in the hamilton
    driver."""

    pass


def _set_materializer_attrs():
    """Sets materialization attributes for easy reference. This sets it to the available keys.
    This is so one can get auto-complete"""

    def with_modified_signature(
        fn: Type[_FactoryProtocol], dataclasses_union: List[Type[dataclasses.dataclass]]
    ):
        """Modifies the signature to include the parameters from *all* dataclasses.
        Note this just replaces **kwargs with the union of the parameters. Its not
        strictly correct, as (a) its a superset of the available ones and (b) it doesn't
        include the source/value parameters. However, this can help the development experience
        on jupyter notebooks, and is a good enough approximation for now.


        :param fn: Function to modify -- will change the signature.
        :param dataclasses_union: All dataclasses to union.
        :return: The function without **kwargs and with the union of the parameters.
        """
        original_signature = inspect.signature(fn)
        original_parameters = list(original_signature.parameters.values())

        new_parameters = []
        seen = set()
        for dataclass in dataclasses_union:
            for field in dataclasses.fields(dataclass):
                if field.name not in seen:
                    new_parameters.append(
                        inspect.Parameter(
                            field.name,
                            inspect.Parameter.KEYWORD_ONLY,
                            annotation=field.type,
                            default=None,
                        )
                    )
                seen.add(field.name)

        # Combining old and new parameters
        # Checking for position of **kwargs and insert new params before
        for idx, param in enumerate(original_parameters):
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                break
        else:
            idx = len(original_parameters)

        # Insert new parameters while respecting the order
        combined_parameters = original_parameters[:idx] + new_parameters + original_parameters[idx:]
        combined_parameters = [param for param in combined_parameters if param.name != "kwargs"]

        # Creating a new signature with combined parameters
        new_signature = original_signature.replace(parameters=combined_parameters)

        # Creating a new function with the new signature
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound_arguments = new_signature.bind(*args, **kwargs)
            return fn(*bound_arguments.args, **bound_arguments.kwargs)

        # Assign the new signature to the wrapper function
        wrapper.__signature__ = new_signature
        wrapper.__doc__ = f"""
        Materializes data to {key} format. Note that the parameters are a superset of possible parameters -- this might depend on
        the actual type of the data passed in. For more information, see: https://hamilton.dagworks.io/en/latest/reference/io/available-data-adapters/#data-loaders.
        You can also pass `source` and `value` in as kwargs.
        """
        return wrapper

    # only go through the savers as those are the targets of materializers
    for key, item in SAVER_REGISTRY.items():
        potential_loaders = SAVER_REGISTRY[key]
        savers = [loader for loader in potential_loaders if issubclass(loader, DataSaver)]
        if len(savers) > 0:
            partial = partial_materializer(SAVER_REGISTRY[key])
            partial_with_signature = with_modified_signature(partial, SAVER_REGISTRY[key])
            setattr(Materialize, key, partial_with_signature)
            Materialize.__annotations__[key] = type(partial_with_signature)


_set_materializer_attrs()


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
