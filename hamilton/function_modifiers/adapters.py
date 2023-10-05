import inspect
import logging
import typing
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type

from hamilton import node
from hamilton.function_modifiers.base import (
    InvalidDecoratorException,
    NodeInjector,
    SingleNodeNodeTransformer,
)
from hamilton.function_modifiers.dependencies import (
    LiteralDependency,
    ParametrizedDependency,
    UpstreamDependency,
)
from hamilton.io.data_adapters import AdapterCommon, DataLoader, DataSaver
from hamilton.node import DependencyType
from hamilton.registry import LOADER_REGISTRY, SAVER_REGISTRY

logger = logging.getLogger(__name__)


class AdapterFactory:
    """Factory for data loaders. This handles the fact that we pass in source(...) and value(...)
    parameters to the data loaders."""

    def __init__(self, adapter_cls: Type[AdapterCommon], **kwargs: ParametrizedDependency):
        """Initializes an adapter factory. This takes in parameterized dependencies
        and stores them for later resolution.

        Note that this is not strictly necessary -- we could easily put this in the
        decorator, but I wanted to separate out/consolidate the logic between data savers and data
        loaders.

        :param adapter_cls: Class of the loader to create.
        :param kwargs: Keyword arguments to pass to the loader, as parameterized dependencies.
        """
        self.adapter_cls = adapter_cls
        self.kwargs = kwargs
        self.validate()

    def validate(self):
        """Validates that the loader class has the required arguments, and that
        the arguments passed in are valid.

        :raises InvalidDecoratorException: If the arguments are invalid.
        """
        required_args = self.adapter_cls.get_required_arguments()
        optional_args = self.adapter_cls.get_optional_arguments()
        missing_params = set(required_args.keys()) - set(self.kwargs.keys())
        extra_params = (
            set(self.kwargs.keys()) - set(required_args.keys()) - set(optional_args.keys())
        )
        if len(missing_params) > 0:
            raise InvalidDecoratorException(
                f"Missing required parameters for adapter : {self.adapter_cls}: {missing_params}. "
                f"Required parameters/types are: {required_args}. Optional parameters/types are: "
                f"{optional_args}. "
            )
        if len(extra_params) > 0:
            available_args = {**required_args, **optional_args}.keys()
            raise InvalidDecoratorException(
                f"Extra parameters for loader: {self.adapter_cls} {extra_params}. Choices for parameters are: "
                f"{available_args}."
            )

    def create_loader(self, **resolved_kwargs: Any) -> DataLoader:
        if not self.adapter_cls.can_load():
            raise InvalidDecoratorException(f"Adapter {self.adapter_cls} cannot load data.")
        return self.adapter_cls(**resolved_kwargs)

    def create_saver(self, **resolved_kwargs: Any) -> DataSaver:
        if not self.adapter_cls.can_save():
            raise InvalidDecoratorException(f"Adapter {self.adapter_cls} cannot save data.")
        return self.adapter_cls(**resolved_kwargs)


def resolve_kwargs(kwargs: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """Resolves kwargs to a list of dependencies, and a dictionary of name
    to resolved literal values.

    :return: A tuple of the dependencies, and the resolved literal kwargs.
    """
    dependencies = {}
    resolved_kwargs = {}
    for name, dependency in kwargs.items():
        if isinstance(dependency, UpstreamDependency):
            dependencies[name] = dependency.source
        elif isinstance(dependency, LiteralDependency):
            resolved_kwargs[name] = dependency.value
        else:
            resolved_kwargs[name] = dependency
    return dependencies, resolved_kwargs


def resolve_adapter_class(
    type_: Type[Type], loader_classes: List[Type[AdapterCommon]]
) -> Optional[Type[AdapterCommon]]:
    """Resolves the loader class for a function. This will return the most recently
    registered loader class that applies to the injection type, hence the reversed order.

    :param fn: Function to inject the loaded data into.
    :return: The loader class to use.
    """
    applicable_adapters: List[Type[AdapterCommon]] = []
    for loader_cls in reversed(loader_classes):
        if loader_cls.applies_to(type_):
            applicable_adapters.append(loader_cls)
    if len(applicable_adapters) > 0:
        if len(applicable_adapters) > 1:
            logger.warning(
                f"More than one applicable adapter detected for {type_}. "
                f"Using the last one registered {applicable_adapters[0]}."
            )
        return applicable_adapters[0]
    return None


class LoadFromDecorator(NodeInjector):
    def __init__(
        self,
        loader_classes: typing.Sequence[Type[DataLoader]],
        inject_=None,
        **kwargs: ParametrizedDependency,
    ):
        """Instantiates a load_from decorator. This decorator will load from a data source,
        and

        :param inject: The name of the parameter to inject the data into.
        :param loader_cls: The data loader class to use.
        :param kwargs: The arguments to pass to the data loader.
        """
        self.loader_classes = loader_classes
        self.kwargs = kwargs
        self.inject = inject_

    def _select_param_to_inject(self, params: List[str], fn: Callable) -> str:
        """Chooses a parameter to inject, given the parameters available. If self.inject is None
        (meaning we inject the only parameter), then that's the one. If it is not None, then
        we need to ensure it is one of the available parameters, in which case we choose it."""
        if self.inject is None:
            if len(params) == 1:
                return params[0]
            raise InvalidDecoratorException(
                f"If the nodes produced by {fn.__qualname__} require multiple inputs, "
                f"you must pass `inject_` to the load_from decorator for "
                f"function: {fn.__qualname__}"
            )
        if self.inject not in params:
            raise InvalidDecoratorException(
                f"Parameter {self.inject} not required by nodes produced by {fn.__qualname__}"
            )
        return self.inject

    def inject_nodes(
        self, params: Dict[str, Type[Type]], config: Dict[str, Any], fn: Callable
    ) -> Tuple[Collection[node.Node], Dict[str, str]]:
        pass
        """Generates two nodes:
        1. A node that loads the data from the data source, and returns that + metadata
        2. A node that takes the data from the data source, injects it into, and runs, the function.

        :param fn: The function to decorate.
        :param config: The configuration to use.
        :return: The resolved nodes
        """
        inject_parameter = self._select_param_to_inject(list(params.keys()), fn)
        load_type = params[inject_parameter]
        loader_cls = resolve_adapter_class(
            load_type,
            self.loader_classes,
        )
        if loader_cls is None:
            raise InvalidDecoratorException(
                f"If you have multiple parameters required, "
                f"you must pass `inject_` to the load_from decorator for "
                f"function: {fn.__qualname__}"
            )
        loader_factory = AdapterFactory(loader_cls, **self.kwargs)
        # dependencies is a map from param name -> source name
        # we use this to pass the right arguments to the loader.
        dependencies, resolved_kwargs = resolve_kwargs(self.kwargs)
        # we need to invert the dependencies so that we can pass
        # the right argument to the loader
        dependencies_inverted = {v: k for k, v in dependencies.items()}

        def load_data(
            __loader_factory: AdapterFactory = loader_factory,
            __load_type: Type[Type] = load_type,
            __resolved_kwargs=resolved_kwargs,
            __dependencies=dependencies_inverted,
            __optional_params=loader_cls.get_optional_arguments(),
            **input_kwargs: Any,
        ) -> Tuple[load_type, Dict[str, Any]]:
            input_args_with_fixed_dependencies = {
                __dependencies.get(key, key): value for key, value in input_kwargs.items()
            }
            kwargs = {**__resolved_kwargs, **input_args_with_fixed_dependencies}
            data_loader = __loader_factory.create_loader(**kwargs)
            return data_loader.load_data(load_type)

        def get_input_type_key(key: str) -> str:
            return key if key not in dependencies else dependencies[key]

        input_types = {
            get_input_type_key(key): (type_, DependencyType.REQUIRED)
            for key, type_ in loader_cls.get_required_arguments().items()
        }
        input_types.update(
            {
                dependencies[key]: (type_, DependencyType.OPTIONAL)
                for key, type_ in loader_cls.get_optional_arguments().items()
                if key in dependencies
            }
        )
        # Take out all the resolved kwargs, as they are not dependencies, and will be filled out
        # later
        input_types = {
            key: value for key, value in input_types.items() if key not in resolved_kwargs
        }

        # the loader node is the node that loads the data from the data source.
        loader_node = node.Node(
            name=f"{inject_parameter}",
            callabl=load_data,
            typ=Tuple[Dict[str, Any], load_type],
            input_types=input_types,
            tags={
                "hamilton.data_loader": True,
                "hamilton.data_loader.has_metadata": True,
                "hamilton.data_loader.source": f"{loader_cls.name()}",
                "hamilton.data_loader.classname": f"{loader_cls.__qualname__}",
                "hamilton.data_loader.node": inject_parameter,
            },
            namespace=(fn.__name__, "load_data"),
        )

        # the filter node is the node that takes the data from the data source, filters out
        # metadata, and feeds it in as a parameter. Note that this must have the same name as the
        # inject parameter

        def filter_function(_inject_parameter=inject_parameter, **kwargs):
            return kwargs[loader_node.name][0]  # filter it out

        filter_node = node.Node(
            name=f"{inject_parameter}",
            callabl=filter_function,
            typ=load_type,
            input_types={loader_node.name: loader_node.type},
            tags={
                "hamilton.data_loader": True,
                "hamilton.data_loader.has_metadata": False,
                "hamilton.data_loader.source": f"{loader_cls.name()}",
                "hamilton.data_loader.classname": f"{loader_cls.__qualname__}",
                "hamilton.data_loader.node": inject_parameter,
            },
            namespace=(fn.__name__, "select_data"),
        )
        return [loader_node, filter_node], {inject_parameter: filter_node.name}

    def _get_inject_parameter_from_function(self, fn: Callable) -> Tuple[str, Type[Type]]:
        """Gets the name of the parameter to inject the data into.

        :param fn: The function to decorate.
        :return: The name of the parameter to inject the data into.
        """
        sig = inspect.signature(fn)
        if self.inject is None:
            if len(sig.parameters) == 0:
                raise InvalidDecoratorException(
                    f"The function: {fn.__qualname__} has no parameters. "
                    f"The data loader functionality injects the loaded data "
                    f"into the function, so you must have at least one parameter."
                )
            if len(sig.parameters) != 1:
                raise InvalidDecoratorException(
                    f"If you have multiple parameters in the signature, "
                    f"you must pass `inject_` to the load_from decorator for "
                    f"function: {fn.__qualname__}"
                )
            inject = list(sig.parameters.keys())[0]

        else:
            if self.inject not in sig.parameters:
                raise InvalidDecoratorException(
                    f"Invalid inject parameter: {self.inject} for fn: {fn.__qualname__}"
                )
            inject = self.inject
        return inject, typing.get_type_hints(fn)[inject]

    def validate(self, fn: Callable):
        """Note that we actually do something a little clever here. While the decorator operates
        on the nodes in the subdag, we actually validate it against the function. This ensures that
        we're not doing a lot of weird dynamic things that inject parameters that aren't in the
        function.

        :param fn: Function that produces the data loader.
        """

        inject_parameter, type_ = self._get_inject_parameter_from_function(fn)
        cls = resolve_adapter_class(type_, self.loader_classes)
        if cls is None:
            raise InvalidDecoratorException(
                f"No loader class found for type: {type_} specified by "
                f"parameter: {inject_parameter} in function: {fn.__qualname__}"
            )
        loader_factory = AdapterFactory(cls, **self.kwargs)
        loader_factory.validate()


class load_from__meta__(type):
    """Metaclass for the load_from decorator. This is specifically to allow class access method.
    Note that there *is* another way to do this -- we couold add attributes dynamically on the
    class in registry, or make it a function that just proxies to the decorator. We can always
    change this up, but this felt like a pretty clean way of doing it, where we can decouple the
    registry from the decorator class.
    """

    def __getattr__(cls, item: str):
        if item in LOADER_REGISTRY:
            return load_from.decorator_factory(LOADER_REGISTRY[item])
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(
                f"No loader named: {item} available for {cls.__name__}. "
                f"Available loaders are: {LOADER_REGISTRY.keys()}. "
                f"If you've gotten to this point, you either (1) spelled the "
                f"loader name wrong, (2) are trying to use a loader that does"
                f"not exist (yet). For a list of available loaders, see: "
                f"https://hamilton.readthedocs.io/reference/io/available-data-adapters/#data"
                f"-loaders "
            ) from e


class load_from(metaclass=load_from__meta__):
    """Decorator to inject externally loaded data into a function. Ideally, anything that is not
    a pure transform should either call this, or accept inputs from an external location.

    This decorator functions by "injecting" a parameter into the function. For example,
    the following code will load the json file, and inject it into the function as the parameter
    `input_data`. Note that the path for the JSON file comes from another node called
    raw_data_path (which could also be passed in as an external input).

    .. code-block:: python

        @load_from.json(path=source("raw_data_path"))
        def raw_data(input_data: dict) -> dict:
            return input_data

    The decorator can also be used with `value` to inject a constant value into the loader.
    In the following case, we use the literal value "some/path.json" as the path to the JSON file.

    .. code-block:: python

        @load_from.json(path=value("some/path.json"))
        def raw_data(input_data: dict) -> dict:
            return input_data

    Note that, if neither `source` nor `value` is specified, the value will be passed in as a
    literal value.

    .. code-block:: python

        @load_from.json(path="some/path.json")
        def raw_data(input_data: dict) -> dict:
            return input_data

    You can also utilize the `inject_` parameter in the loader if you want to inject the data
    into a specific param. For example, the following code will load the json file, and inject it
    into the function as the parameter `data`.

    .. code-block:: python

        @load_from.json(path=source("raw_data_path"), inject_="data")
        def raw_data(data: dict, valid_keys: List[str]) -> dict:
            return [item for item in data if item in valid_keys]

    You can also utilize multiple data loaders with separate `inject_` parameters to
    load from multiple files.
    data loaders to a single function:

    .. code-block:: python

        @load_from.json(path=source("raw_data_path"), inject_="data")
        @load_from.json(path=source("raw_data_path2"), inject_="data2")
        def raw_data(data: dict, data2: dict) -> dict:
            return [item for item in data if item in data2]


    This is a highly pluggable functionality -- here's the basics of how it works:

    1. Every "key" (json above, but others include csv, literal, file, pickle, etc...) corresponds
    to a set of loader classes. For example, the json key corresponds to the JSONLoader class in
    default_data_loaders. They implement the classmethod `name`. Once they are registered with the
    central registry they pick

    2. Every data loader class (which are all dataclasses) implements the `load_targets` method,
    which returns a list of types it can load to. For example, the JSONLoader class can load data
    of type `dict`. Note that the set of potential loading candidate classes are evaluated in
    reverse order, so the most recently registered loader class is the one that is used. That
    way, you can register custom ones.

    3. The loader class is instantiated with the kwargs passed to the decorator. For example, the
    JSONLoader class takes a `path` kwarg, which is the path to the JSON file.

    4. The decorator then creates a node that loads the data, and modifies the node that runs the
    function to accept that. It also returns metadata (customizable at the loader-class-level) to
    enable debugging after the fact. This is unstructured, but can be used down the line to describe
    any metadata to help debug.

    The "core" hamilton library contains a few basic data loaders that can be implemented within
    the confines of python's standard library. pandas_extensions contains a few more that require
    pandas to be installed.

    Note that these can have `default` arguments, specified by defaults in the dataclass fields.
    For the full set of "keys" and "types" (e.g. load_from.json, etc...), look for all classes
    that inherit from `DataLoader` in the hamilton library. We plan to improve documentation shortly
    to make this discoverable.
    """

    def __call__(self, *args, **kwargs):
        return LoadFromDecorator(*args, **kwargs)

    @classmethod
    def decorator_factory(
        cls, loaders: typing.Sequence[Type[DataLoader]]
    ) -> Callable[..., LoadFromDecorator]:
        """Effectively a partial function for the load_from decorator. Broken into its own (
        rather than using functools.partial) as it is a little clearer to parse.

        :param loaders: Options of data loader classes to use
        :return: The data loader decorator.
        """

        def create_decorator(
            __loaders=tuple(loaders), inject_=None, **kwargs: ParametrizedDependency
        ):
            return LoadFromDecorator(__loaders, inject_=inject_, **kwargs)

        return create_decorator


class save_to__meta__(type):
    """See note on load_from__meta__ for details on how this works."""

    def __getattr__(cls, item: str):
        if item in SAVER_REGISTRY:
            return save_to.decorator_factory(SAVER_REGISTRY[item])
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(
                "No saver named: {item} available for {cls.__name__}. "
                "Available data savers are: {list(SAVER_REGISTRY.keys())}. "
                "If you've gotten to this point, you either (1) spelled the "
                "loader name wrong, (2) are trying to use a saver that does"
                "not exist (yet). For a list of available savers, see "
                "https://hamilton.readthedocs.io/reference/io/available-data-adapters/#data"
                "-loaders "
            ) from e


class SaveToDecorator(SingleNodeNodeTransformer):
    def __init__(
        self,
        saver_classes_: typing.Sequence[Type[DataSaver]],
        output_name_: str = None,
        **kwargs: ParametrizedDependency,
    ):
        super(SaveToDecorator, self).__init__()
        self.artifact_name = output_name_
        self.saver_classes = saver_classes_
        self.kwargs = kwargs

    def create_saver_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> node.Node:
        artifact_name = self.artifact_name
        artifact_namespace = ()

        if artifact_name is None:
            artifact_name = node_.name
            artifact_namespace = ("save",)

        type_ = node_.type
        saver_cls = resolve_adapter_class(
            type_,
            self.saver_classes,
        )
        if saver_cls is None:
            raise InvalidDecoratorException(
                f"No saver class found for type: {type_} specified by "
                f"output type: {type_} in node: {node_.name} generated by "
                f"function: {fn.__qualname__}."
            )

        adapter_factory = AdapterFactory(saver_cls, **self.kwargs)
        dependencies, resolved_kwargs = resolve_kwargs(self.kwargs)
        dependencies_inverted = {v: k for k, v in dependencies.items()}

        def save_data(
            __adapter_factory=adapter_factory,
            __dependencies=dependencies_inverted,
            __resolved_kwargs=resolved_kwargs,
            __data_node_name=node_.name,
            **input_kwargs,
        ) -> Dict[str, Any]:
            input_args_with_fixed_dependencies = {
                __dependencies.get(key, key): value for key, value in input_kwargs.items()
            }
            kwargs = {**__resolved_kwargs, **input_args_with_fixed_dependencies}
            data_to_save = kwargs[__data_node_name]
            kwargs = {k: v for k, v in kwargs.items() if k != __data_node_name}
            data_saver = __adapter_factory.create_saver(**kwargs)
            return data_saver.save_data(data_to_save)

        def get_input_type_key(key: str) -> str:
            return key if key not in dependencies else dependencies[key]

        input_types = {
            get_input_type_key(key): (type_, DependencyType.REQUIRED)
            for key, type_ in saver_cls.get_required_arguments().items()
        }
        input_types.update(
            {
                dependencies[key]: (type_, DependencyType.OPTIONAL)
                for key, type_ in saver_cls.get_optional_arguments().items()
                if key in dependencies
            }
        )
        # Take out all the resolved kwargs, as they are not dependencies, and will be filled out
        # later
        input_types = {
            key: value for key, value in input_types.items() if key not in resolved_kwargs
        }
        input_types[node_.name] = (node_.type, DependencyType.REQUIRED)

        save_node = node.Node(
            name=artifact_name,
            callabl=save_data,
            typ=Dict[str, Any],
            input_types=input_types,
            namespace=artifact_namespace,
            tags={
                "hamilton.data_saver": True,
                "hamilton.data_saver.sink": f"{saver_cls.name()}",
                "hamilton.data_saver.classname": f"{saver_cls.__qualname__}",
            },
        )
        return save_node

    def transform_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """Transforms the node to a data saver.

        :param node_: Node to transform
        :param config: Config to use
        :param fn: Original function that generated the node
        :return: The transformed node
        """

        return [
            self.create_saver_node(node_, config, fn),
            node_,
        ]

    def validate(self, fn: Callable):
        pass


class save_to(metaclass=save_to__meta__):
    """Decorator that outputs data to some external source. You can think
    about this as the inverse of load_from.

    This decorates a function, takes the final node produced by that function
    and then appends an additional node that saves the output of that function.

    As the load_from decorator does, this decorator can be referred to in a
    dynamic way. For instance, @save_to.json will save the output of the function
    to a json file. Note that this means that the output of the function must
    be a dictionary (or subclass thereof), otherwise the decorator will fail.

    Looking at the json example:

    .. code-block:: python

        @save_to.json(path=source("raw_data_path"), output_name_="data_save_output")
        def final_output(data: dict, valid_keys: List[str]) -> dict:
            return [item for item in data if item in valid_keys]

    This adds a final node to the DAG with the name "data_save_output" that
    accepts the output of the function "final_output" and saves it to a json.
    In this case, the JSONSaver accepts a `path` parameter, which is provided
    by the upstream node (or input) named "raw_data_path". The `artifact_`
    parameter then says how to refer to the output of this node in the DAG.

    If you called this with the driver:

    .. code-block:: python

        dr = driver.Driver(my_module)
        output = dr.execute(['final_output'], {'raw_data_path': '/path/my_data.json'})

    You would *just* get the final result, and nothing would be saved.

    If you called this with the driver:

    .. code-block:: python

        dr = driver.Driver(my_module)
        output = dr.execute(['data_save_output'], {'raw_data_path': '/path/my_data.json'})

    You would get a dictionary of metadata (about the saving output), and the final result would
    be saved to a path.

    Note that you can also hardcode the path, rather than using a dependency:

    .. code-block:: python

        @save_to.json(path=value('/path/my_data.json'), output_name_="data_save_output")
        def final_output(data: dict, valid_keys: List[str]) -> dict:
            return [item for item in data if item in valid_keys]

    Note that, like the loader function, you can use literal values as kwargs and they'll get interpreted as values.
    If you needs savers, you should also look into `.materialize` on the driver -- it's a clean way to do this in a more
    ad-hoc/decoupled manner.
    """

    def __call__(self, *args, **kwargs):
        return LoadFromDecorator(*args, **kwargs)

    @classmethod
    def decorator_factory(
        cls, savers: typing.Sequence[Type[DataSaver]]
    ) -> Callable[..., SaveToDecorator]:
        """Effectively a partial function for the load_from decorator. Broken into its own (
        rather than using functools.partial) as it is a little clearer to parse.

        :param savers: Candidate data savers
        :param loaders: Options of data loader classes to use
        :return: The data loader decorator.
        """

        def create_decorator(__savers=tuple(savers), **kwargs: ParametrizedDependency):
            return SaveToDecorator(__savers, **kwargs)

        return create_decorator
