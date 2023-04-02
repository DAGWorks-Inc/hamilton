import inspect
import typing
from typing import Any, Callable, Dict, List, Tuple, Type

from hamilton import node
from hamilton.function_modifiers.base import InvalidDecoratorException, NodeCreator
from hamilton.function_modifiers.dependencies import (
    LiteralDependency,
    ParametrizedDependency,
    UpstreamDependency,
)
from hamilton.io.data_loaders import DataLoader
from hamilton.node import DependencyType
from hamilton.registry import ADAPTER_REGISTRY


class LoaderFactory:
    """Factory for data loaders. This handles the fact that we pass in source(...) and value(...)
    parameters to the data loaders."""

    def __init__(self, loader_cls: Type[DataLoader], **kwargs: ParametrizedDependency):
        """Initializes a loader factory. This takes in parameterized dependencies
        and stores them for later resolution.

        Note that this is not strictly necessary -- we could easily put this in the

        :param loader_cls: Class of the loader to create.
        :param kwargs: Keyword arguments to pass to the loader, as parameterized dependencies.
        """
        self.loader_cls = loader_cls
        self.kwargs = kwargs
        self.validate()

    def validate(self):
        """Validates that the loader class has the required arguments, and that
        the arguments passed in are valid.

        :raises InvalidDecoratorException: If the arguments are invalid.
        """
        required_args = self.loader_cls.get_required_arguments()
        optional_args = self.loader_cls.get_optional_arguments()
        missing_params = set(required_args.keys()) - set(self.kwargs.keys())
        extra_params = (
            set(self.kwargs.keys()) - set(required_args.keys()) - set(optional_args.keys())
        )
        if len(missing_params) > 0:
            raise InvalidDecoratorException(
                f"Missing required parameters for loader : {self.loader_cls}: {missing_params}. "
                f"Required parameters/types are: {required_args}. Optional parameters/types are: "
                f"{optional_args}. "
            )
        if len(extra_params) > 0:
            raise InvalidDecoratorException(
                f"Extra parameters for loader: {self.loader_cls} {extra_params}"
            )

    def create_loader(self, **resolved_kwargs: Any) -> DataLoader:
        return self.loader_cls(**resolved_kwargs)


class LoadFromDecorator(NodeCreator):
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

    def resolve_kwargs(self) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """Resolves kwargs to a list of dependencies, and a dictionary of name
        to resolved literal values.

        :return: A tuple of the dependencies, and the resolved literal kwargs.
        """
        dependencies = {}
        resolved_kwargs = {}
        for name, dependency in self.kwargs.items():
            if isinstance(dependency, UpstreamDependency):
                dependencies[name] = dependency.source
            elif isinstance(dependency, LiteralDependency):
                resolved_kwargs[name] = dependency.value
        return dependencies, resolved_kwargs

    def generate_nodes(self, fn: Callable, config: Dict[str, Any]) -> List[node.Node]:
        """Generates two nodes:
        1. A node that loads the data from the data source, and returns that + metadata
        2. A node that takes the data from the data source, injects it into, and runs, the function.

        :param fn: The function to decorate.
        :param config: The configuration to use.
        :return: The resolved nodes
        """
        loader_cls = self._resolve_loader_class(fn)
        loader_factory = LoaderFactory(loader_cls, **self.kwargs)
        # dependencies is a map from param name -> source name
        # we use this to pass the right arguments to the loader.
        dependencies, resolved_kwargs = self.resolve_kwargs()
        # we need to invert the dependencies so that we can pass
        # the right argument to the loader
        dependencies_inverted = {v: k for k, v in dependencies.items()}
        inject_parameter, load_type = self._get_inject_parameter(fn)

        def load_data(
            __loader_factory: LoaderFactory = loader_factory,
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
            get_input_type_key(key): (Any, DependencyType.REQUIRED)
            for key in loader_cls.get_required_arguments()
        }
        input_types.update(
            {
                (get_input_type_key(key) if key not in dependencies else dependencies[key]): (
                    Any,
                    DependencyType.OPTIONAL,
                )
                for key in loader_cls.get_optional_arguments()
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
            namespace=("load_data", fn.__name__),
        )

        # the inject node is the node that takes the data from the data source, and injects it into
        # the function.

        def inject_function(**kwargs):
            new_kwargs = kwargs.copy()
            new_kwargs[inject_parameter] = kwargs[loader_node.name][0]
            del new_kwargs[loader_node.name]
            return fn(**new_kwargs)

        raw_node = node.Node.from_fn(fn)
        new_input_types = {
            (key if key != inject_parameter else loader_node.name): loader_node.type
            for key, value in raw_node.input_types.items()
        }
        data_node = raw_node.copy_with(
            input_types=new_input_types,
            callabl=inject_function,
        )
        return [loader_node, data_node]

    def _get_inject_parameter(self, fn: Callable) -> Tuple[str, Type[Type]]:
        """Gets the name of the parameter to inject the data into.

        :param fn: The function to decorate.
        :return: The name of the parameter to inject the data into.
        """
        sig = inspect.signature(fn)
        if self.inject is None:
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
        """Validates the decorator. Currently this just cals the get_inject_parameter and
        cascades the error which is all we know at validation time.

        :param fn:
        :return:
        """
        self._get_inject_parameter(fn)
        cls = self._resolve_loader_class(fn)
        loader_factory = LoaderFactory(cls, **self.kwargs)
        loader_factory.validate()

    def _resolve_loader_class(self, fn: Callable) -> Type[DataLoader]:
        """Resolves the loader class for a function. This will return the most recently
        registered loader class that applies to the injection type, hence the reversed order.

        :param fn: Function to inject the loaded data into.
        :return: The loader class to use.
        """
        param, type_ = self._get_inject_parameter(fn)
        for loader_cls in reversed(self.loader_classes):
            if loader_cls.applies_to(type_):
                return loader_cls

        raise InvalidDecoratorException(
            f"No loader class found for type: {type_} specified by "
            f"parameter: {param} in function: {fn.__qualname__}"
        )


class load_from__meta__(type):
    def __getattr__(cls, item: str):
        if item in ADAPTER_REGISTRY:
            return load_from.decorator_factory(ADAPTER_REGISTRY[item])
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(
                f"No loader named: {item} available for {cls.__name__}. "
                f"Available loaders are: {ADAPTER_REGISTRY.keys()}. "
                f"If you've gotten to this point, you either (1) spelled the "
                f"loader name wrong, (2) are trying to use a loader that does"
                f"not exist (yet), or (3) have implemented it and "
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

    You can also utilize the `inject_` parameter in the loader if you want to inject the data
    into a specific param. For example, the following code will load the json file, and inject it
    into the function as the parameter `data`.

    .. code-block:: python

        @load_from.json(path=source("raw_data_path"), inject_="data")
        def raw_data(data: dict, valid_keys: List[str]) -> dict:
            return [item for item in data if item in valid_keys]


    This is a highly pluggable functionality -- here's the basics of how it works:

    1. Every "key" (json above, but others include csv, literal, file, pickle, etc...) corresponds
    to a set of loader classes. For example, the json key corresponds to the JSONLoader class in
    default_data_loaders. They implement the classmethod `name`. Once they are registered with the
    central registry they pick

    2. Every data loader class (which are all dataclasses) implements the `applies_to` method,
    which takes a type and returns True if it can load data of that type. For example,
    the JSONLoader class can load data of type `dict`. Note that the applies_to is read in
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
