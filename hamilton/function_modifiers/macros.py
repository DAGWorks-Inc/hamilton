import inspect
import logging
import typing
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from hamilton import models, node
from hamilton.dev_utils.deprecation import deprecated
from hamilton.function_modifiers import base
from hamilton.function_modifiers.configuration import ConfigResolver
from hamilton.function_modifiers.delayed import resolve as delayed_resolve
from hamilton.function_modifiers.dependencies import (
    LiteralDependency,
    SingleDependency,
    UpstreamDependency,
    source,
)

logger = logging.getLogger(__name__)

"""Decorators that replace a function's execution with specified behavior"""

# Python 3.10 + has this built in, otherwise we have to define it
try:
    from types import EllipsisType
except ImportError:
    EllipsisType = type(...)


# the following are empty functions that we can compare against to ensure that @does uses an empty function
def _empty_function():
    pass


def _empty_function_with_docstring():
    """Docstring for an empty function"""
    pass


def ensure_function_empty(fn: Callable):
    """
    Ensures that a function is empty. This is strict definition -- the function must have only one line (and
    possibly a docstring), and that line must say "pass".
    """
    if fn.__code__.co_code not in {
        _empty_function.__code__.co_code,
        _empty_function_with_docstring.__code__.co_code,
    }:
        raise base.InvalidDecoratorException(
            f"Function: {fn.__name__} is not empty. Must have only one line that "
            'consists of "pass"'
        )


class does(base.NodeCreator):
    """``@does`` is a decorator that essentially allows you to run a function over all the input parameters. \
    So you can't pass any old function to ``@does``, instead the function passed has to take any amount of inputs and \
    process them all in the same way.

    .. code-block:: python

        import pandas as pd
        from hamilton.function_modifiers import does
        import internal_package_with_logic

        def sum_series(**series: pd.Series) -> pd.Series:
            '''This function takes any number of inputs and sums them all together.'''
            ...

        @does(sum_series)
        def D_XMAS_GC_WEIGHTED_BY_DAY(D_XMAS_GC_WEIGHTED_BY_DAY_1: pd.Series,
                                      D_XMAS_GC_WEIGHTED_BY_DAY_2: pd.Series) -> pd.Series:
            '''Adds D_XMAS_GC_WEIGHTED_BY_DAY_1 and D_XMAS_GC_WEIGHTED_BY_DAY_2'''
            pass

        @does(internal_package_with_logic.identity_function)
        def copy_of_x(x: pd.Series) -> pd.Series:
            '''Just returns x'''
            pass

    The example here is a function, that all that it does, is sum all the parameters together. So we can annotate it \
    with the ``@does`` decorator and pass it the ``sum_series`` function. The ``@does`` decorator is currently limited \
    to just allow functions that consist only of one argument, a generic \\*\\*kwargs.
    """

    def __init__(self, replacing_function: Callable, **argument_mapping: Union[str, List[str]]):
        """Constructor for a modifier that replaces the annotated functions functionality with something else.
        Right now this has a very strict validation requirements to make compliance with the framework easy.

        :param replacing_function: The function to replace the original function with.
        :param argument_mapping: A mapping of argument name in the replacing function to argument name in the \
        decorating function.
        """
        self.replacing_function = replacing_function
        self.argument_mapping = argument_mapping

    @staticmethod
    def map_kwargs(kwargs: Dict[str, Any], argument_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Maps kwargs using the argument mapping.
        This does 2 things:
        1. Replaces all kwargs in passed_in_kwargs with their mapping
        2. Injects all defaults from the origin function signature

        :param kwargs: Keyword arguments that will be passed into a hamilton function.
        :param argument_mapping: Mapping of those arguments to a replacing function's arguments.
        :return: The new kwargs for the replacing function's arguments.
        """
        output = {**kwargs}
        for arg_mapped_to, original_arg in argument_mapping.items():
            if original_arg in kwargs and arg_mapped_to not in argument_mapping.values():
                del output[original_arg]
            # Note that if it is not there it could be a **kwarg
            output[arg_mapped_to] = kwargs[original_arg]
        return output

    @staticmethod
    def test_function_signatures_compatible(
        fn_signature: inspect.Signature,
        replace_with_signature: inspect.Signature,
        argument_mapping: Dict[str, str],
    ) -> bool:
        """Tests whether a function signature and the signature of the replacing function are compatible.

        :param fn_signature:
        :param replace_with_signature:
        :param argument_mapping:
        :return: True if they're compatible, False otherwise
        """
        # The easy (and robust) way to do this is to use the bind with a set of dummy arguments and test if it breaks.
        # This way we're not reinventing the wheel.
        SENTINEL_ARG_VALUE = ...  # does not matter as we never use it
        # We initialize as the default values, as they'll always be injected in
        dummy_param_values = {
            key: SENTINEL_ARG_VALUE
            for key, param_spec in fn_signature.parameters.items()
            if param_spec.default is not inspect.Parameter.empty
        }
        # Then we update with the dummy values. Again, replacing doesn't matter (we'll be mimicking it later)
        dummy_param_values.update({key: SENTINEL_ARG_VALUE for key in fn_signature.parameters})
        dummy_param_values = does.map_kwargs(dummy_param_values, argument_mapping)
        try:
            # Python signatures have a bind() capability which does exactly what we want to do
            # Throws a type error if it is not valid
            replace_with_signature.bind(**dummy_param_values)
        except TypeError:
            return False
        return True

    @staticmethod
    def ensure_function_signature_compatible(
        og_function: Callable,
        replacing_function: Callable,
        argument_mapping: Dict[str, str],
    ):
        """Ensures that a function signature is compatible with the replacing function, given the argument mapping

        :param og_function: Function that's getting replaced (decorated with `@does`)
        :param replacing_function: A function that gets called in its place (passed in by `@does`)
        :param argument_mapping: The mapping of arguments from fn to replace_with
        :return:
        """
        fn_parameters = inspect.signature(og_function).parameters
        invalid_fn_parameters = []
        for param_name, param_spec in fn_parameters.items():
            if param_spec.kind not in {
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }:
                invalid_fn_parameters.append(param_name)

        if invalid_fn_parameters:
            raise base.InvalidDecoratorException(
                f"Decorated function for @does (and really, all of hamilton), "
                f"can only consist of keyword-friendly arguments. "
                f"The following parameters for {og_function.__name__} are not keyword-friendly: {invalid_fn_parameters}"
            )
        if not does.test_function_signatures_compatible(
            inspect.signature(og_function),
            inspect.signature(replacing_function),
            argument_mapping,
        ):
            raise base.InvalidDecoratorException(
                f"The following function signatures are not compatible for use with @does: "
                f"{og_function.__name__} with signature {inspect.signature(og_function)} "
                f"and replacing function {replacing_function.__name__} with signature {inspect.signature(replacing_function)}. "
                f"Mapping for arguments provided was: {argument_mapping}. You can fix this by either adjusting "
                f"the signature for the replacing function *or* adjusting the mapping."
            )

    def validate(self, fn: Callable):
        """Validates that the function:
        - Is empty (we don't want to be overwriting actual code)
        - Has a compatible return type
        - Matches the function signature with the appropriate mapping
        :param fn: Function to validate
        :raises: InvalidDecoratorException
        """
        ensure_function_empty(fn)
        does.ensure_function_signature_compatible(
            fn, self.replacing_function, self.argument_mapping
        )

    def generate_nodes(self, fn: Callable, config) -> List[node.Node]:
        """Returns one node which has the replaced functionality
        :param fn: Function to decorate
        :param config: Configuration (not used in this)
        :return: A node with the function in `@does` injected,
        and the same parameters/types as the original function.
        """

        def wrapper_function(**kwargs):
            final_kwarg_values = {
                key: param_spec.default
                for key, param_spec in inspect.signature(fn).parameters.items()
                if param_spec.default is not inspect.Parameter.empty
            }
            final_kwarg_values.update(kwargs)
            final_kwarg_values = does.map_kwargs(final_kwarg_values, self.argument_mapping)
            return self.replacing_function(**final_kwarg_values)

        return [node.Node.from_fn(fn).copy_with(callabl=wrapper_function)]


def get_default_tags(fn: Callable) -> Dict[str, str]:
    """Function that encapsulates default tags on a function.

    :param fn: the function we want to create default tags for.
    :return: a dictionary with str -> str values representing the default tags.
    """
    module_name = inspect.getmodule(fn).__name__
    return {"module": module_name}


@deprecated(
    warn_starting=(1, 20, 0),
    fail_starting=(2, 0, 0),
    use_this=delayed_resolve,
    explanation="dynamic_transform has been replaced with @resolve -- a cleaner way"
    "to utilize config for resolving decorators. Note this allows you to use any"
    "existing decorators.",
    current_version=(1, 19, 0),
    migration_guide="https://hamilton.dagworks.io/en/latest/reference/decorators/",
)
class dynamic_transform(base.NodeCreator):
    def __init__(
        self,
        transform_cls: Type[models.BaseModel],
        config_param: str,
        **extra_transform_params,
    ):
        """Constructs a model. Takes in a model_cls, which has to have a parameter."""
        self.transform_cls = transform_cls
        self.config_param = config_param
        self.extra_transform_params = extra_transform_params

    def validate(self, fn: Callable):
        """Validates that the model works with the function -- ensures:
        1. function has no code
        2. function has no parameters
        3. function has series as a return type
        :param fn: Function to validate
        :raises InvalidDecoratorException if the model is not valid.
        """

        ensure_function_empty(fn)  # it has to look exactly
        signature = inspect.signature(fn)
        if not issubclass(typing.get_type_hints(fn).get("return"), pd.Series):
            raise base.InvalidDecoratorException(
                "Models must declare their return type as a pandas Series"
            )
        if len(signature.parameters) > 0:
            raise base.InvalidDecoratorException(
                "Models must have no parameters -- all are passed in through the config"
            )

    def generate_nodes(self, fn: Callable, config: Dict[str, Any] = None) -> List[node.Node]:
        if self.config_param not in config:
            raise base.InvalidDecoratorException(
                f"Configuration has no parameter: {self.config_param}. Did you define it? If so did you spell it right?"
            )
        fn_name = fn.__name__
        transform = self.transform_cls(
            config[self.config_param], fn_name, **self.extra_transform_params
        )
        return [
            node.Node(
                name=fn_name,
                typ=typing.get_type_hints(fn).get("return"),
                doc_string=fn.__doc__,
                callabl=transform.compute,
                input_types={dep: pd.Series for dep in transform.get_dependents()},
                tags=get_default_tags(fn),
            )
        ]

    def require_config(self) -> List[str]:
        """Returns the configuration parameters that this model requires

        :return: Just the one config param used by this model
        """
        return [self.config_param]


class model(dynamic_transform):
    """Model, same as a dynamic transform"""

    def __init__(self, model_cls, config_param: str, **extra_model_params):
        super(model, self).__init__(
            transform_cls=model_cls, config_param=config_param, **extra_model_params
        )


NamespaceType = Union[str, EllipsisType, None]


class Applicable:
    """Applicable is a largely internal construct that represents a function that can be applied as a node.
    A few of these function are external-facing, however (named, when, when_not, ...)"""

    def __init__(
        self,
        fn: Callable,
        args: Tuple[Union[Any, SingleDependency], ...],
        kwargs: Dict[str, Union[Any, SingleDependency]],
        _resolvers: List[ConfigResolver] = None,
        _name: Optional[str] = None,
        _namespace: Union[str, None, EllipsisType] = ...,
    ):
        """Instantiates an Applicable.

        :param fn: Function it takes in
        :param args: Args (*args) to pass to the function
        :param kwargs: Kwargs (**kwargs) to pass to the function
        :param _resolvers: Resolvers to use for the function
        :param _name: Name of the node to be created
        :param _namespace: Namespace of the node to be created -- currently only single-level namespaces are supported
        """
        self.fn = fn
        if "_name" in kwargs:
            raise ValueError("Cannot pass in _name as a kwarg")

        self.kwargs = {key: value for key, value in kwargs.items() if key != "__name"}  # TODO --
        self.args = args
        # figure out why this was showing up in two places...
        self.resolvers = _resolvers if _resolvers is not None else []
        self.name = _name
        self.namespace = _namespace

    def _with_resolvers(self, *additional_resolvers: ConfigResolver) -> "Applicable":
        """Helper function for the .when* group"""
        return Applicable(
            fn=self.fn,
            _resolvers=self.resolvers + list(additional_resolvers),
            _name=self.name,
            _namespace=self.namespace,
            args=self.args,
            kwargs=self.kwargs,
        )

    def when(self, **key_value_pairs) -> "Applicable":
        """Choose to apply this function when all of the keys in the function
        are present in the config, and the values match the values in the config.

        :param key_value_pairs: key/value pairs to match
        :return: The Applicable with this condition applied
        """
        return self._with_resolvers(ConfigResolver.when(**key_value_pairs))

    def when_not(self, **key_value_pairs) -> "Applicable":
        """Choose to apply this function when all of the keys specified
        do not match the values specified in the config.

        :param key_value_pairs: key/value pairs to match
        :return: The Applicable with this condition applied
        """
        return self._with_resolvers(ConfigResolver.when_not(**key_value_pairs))

    def when_in(self, **key_value_group_pairs: list) -> "Applicable":
        """Choose to apply this function when all the keys provided have values contained within the list of values
        specified

        :param key_value_group_pairs: key/value pairs to match
        :return:  The Applicable with this condition applied
        """
        return self._with_resolvers(ConfigResolver.when_in(**key_value_group_pairs))

    def when_not_in(self, **key_value_group_pairs: list) -> "Applicable":
        """Choose to apply this function when all the keys provided have values not contained within the list of values
        specified.

        :param key_value_group_pairs: key/value pairs to match
        :return:  The Applicable with this condition applied
        """

        return self._with_resolvers(ConfigResolver.when_not_in(**key_value_group_pairs))

    def namespaced(self, namespace: NamespaceType) -> "Applicable":
        """Add a namespace to this node. You probably don't need this -- you should look at "named" instead.

        :param namespace: Namespace to apply, can be ..., None, or a string.
        :return: The Applicable with this namespace
        """
        return Applicable(
            fn=self.fn,
            _resolvers=self.resolvers,
            _name=self.name,
            _namespace=namespace,
            args=self.args,
            kwargs=self.kwargs,
        )

    def resolves(self, config: Dict[str, Any]) -> bool:
        """Returns whether the Applicable resolves with the given config

        :param config: Configuration to check
        :return: Whether the Applicable resolves with the given config
        """
        for resolver in self.resolvers:
            if not resolver(config):
                return False
        return True

    def named(self, name: str, namespace: NamespaceType = ...) -> "Applicable":
        """Names the function application. This has the following rules:
        1. The name will be the name passed in, this is required
        2. If the namespace is `None`, then there will be no namespace
        3. If the namespace is `...`, then the namespace will be the namespace that already exists, usually the name of the
        function that this is decorating. This is an odd case -- but it helps if you have
        multiple of the same type of operations that you want to apply across different nodes,
        or in the case of a parameterization (which is not yet supported).

        :param name: Name of the node to be created
        :param namespace: Namespace of the node to be created -- currently only single-level namespaces are supported
        :return: The applicable with the new name
        """
        return Applicable(
            fn=self.fn,
            _resolvers=self.resolvers,
            _name=name if name is not None else self.name,
            _namespace=(
                None
                if namespace is None
                else (namespace if namespace is not ... else self.namespace)
            ),
            args=self.args,
            kwargs=self.kwargs,
        )

    def get_config_elements(self) -> List[str]:
        """Returns the config elements that this Applicable uses"""
        out = []
        for resolver in self.resolvers:
            out.extend(resolver.optional_config)
        return out

    def validate(self, chain_first_param: bool, allow_custom_namespace: bool):
        """Validates that the Applicable function can be applied given the
        set of args/kwargs passed in. This says that:

        1. The signature binds appropriately
        2. If we chain the first parameter, it is not present in the function

        Note that this is currently restrictive. We only support hamilton-friendly functions. Furthermore,
        this logic is slightly duplicated from `@does` above. We will be suporting more function shapes (in
        both this and `@does`) over time, and also combining the logic between the two for
        validating/binding signatures.

        :param chain_first_param: Whether we chain the first parameter
        :raises InvalidDecoratorException if the function cannot be applied
        :return:
        """
        args = ((...,) if chain_first_param else ()) + tuple(self.args)  # dummy argument at first
        sig = inspect.signature(self.fn)
        if len(sig.parameters) == 0:
            raise base.InvalidDecoratorException(
                f"Function: {self.fn.__name__} has no parameters. "
                f"You cannot apply a function with no parameters."
            )
        invalid_args = [
            item
            for item in inspect.signature(self.fn).parameters.values()
            if item.kind
            not in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }
        ]
        if len(invalid_args) > 0:
            raise base.InvalidDecoratorException(
                f"Function: {self.fn.__name__} has invalid parameters. "
                "You cannot apply a function with parameters that are not keyword-friendly. "
                f"The following parameters are not keyword-friendly: {invalid_args}"
            )
        try:
            sig.bind(*args, **self.kwargs)
        except TypeError as e:
            raise base.InvalidDecoratorException(
                f"Function: {self.fn.__name__} cannot be applied with the following args: {self.args} "
                f"and the following kwargs: {self.kwargs}"
            ) from e
        if len(sig.parameters) == 0:
            raise base.InvalidDecoratorException(
                f"Function: {self.fn.__name__} has no parameters. "
                "You cannot apply a function with no parameters."
            )
        if self.namespace is not ... and not allow_custom_namespace:
            raise base.InvalidDecoratorException(
                "Currently, setting namespace globally inside "
                "pipe(...)/flow(...) is not compatible with setting namespace "
                "for a step(...) call."
            )
        try:
            node.Node.from_fn(self.fn)
        except ValueError as e:
            raise base.InvalidDecoratorException(
                f"Function: {self.fn.__name__} cannot be applied with the following args: {self.args} "
                f"and the following kwargs: {self.kwargs}. See documentation on pipe(), the function "
                "shapes are currently restrictive to anything with named kwargs (either kwarg-only or positional/kwarg arguments), "
                "and must be typed. If you need functions that don't have these requirements, please reach out to the Hamilton team."
                "Current workarounds are to define a wrapper function that assigns types with the proper keyword-friendly arguments."
            ) from e

    def resolve_namespace(self, default_namespace: str) -> Tuple[str, ...]:
        """Resolves the namespace -- see rules in `named` for more details.

        :param default_namespace: namespace to use as a default if we do not wish to override it
        :return: The namespace to use, as a tuple (hierarchical)
        """
        return (
            (default_namespace,)
            if self.namespace is ...
            else (self.namespace,) if self.namespace is not None else ()
        )

    def bind_function_args(
        self, current_param: Optional[str]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Binds function arguments, given current, chained parameeter

        :param current_param: Current, chained parameter. None, if we're not chaining.
        :return: A tuple of (upstream_inputs, literal_inputs)
        """
        args_to_bind = self.args
        if current_param is not None:
            args_to_bind = (source(current_param),) + args_to_bind
        kwargs_to_bind = self.kwargs
        fn_signature = inspect.signature(self.fn)
        bound_signature = fn_signature.bind(*args_to_bind, **kwargs_to_bind)
        all_kwargs = {**bound_signature.arguments, **bound_signature.kwargs}
        upstream_inputs = {}
        literal_inputs = {}
        # TODO -- restrict to ensure that this covers *all* dependencies
        # TODO -- bind to parameters using args
        for dep, value in all_kwargs.items():
            if isinstance(value, UpstreamDependency):
                upstream_inputs[dep] = value.source
            elif isinstance(value, LiteralDependency):
                literal_inputs[dep] = value.value
            else:
                literal_inputs[dep] = value

        return upstream_inputs, literal_inputs


def step(
    fn, *args: Union[SingleDependency, Any], **kwargs: Union[SingleDependency, Any]
) -> Applicable:
    """Applies a function to for a node (or a subcomponent of a node).
    See documentation for `pipe` to see how this is used.

    :param fn: Function to use. Must be validly called as f(**kwargs), and have a 1:1 mapping of kwargs to parameters.
    :param args: Args to pass to the function -- although these cannot be variable/position-only arguments, they can be
    positional arguments. If these are not source/value, they will be converted to a value (a literal)
    :param kwargs: Kwargs to pass to the function. These can be source/value, or they can be literals. If they are literals,
    they will be converted to a value (a literal)
    :return: an applicable with the function applied
    """
    return Applicable(fn=fn, _resolvers=[], args=args, kwargs=kwargs)


class pipe(base.NodeInjector):
    """Decorator to represent a chained set of transformations. This specifically solves the "node redefinition"
    problem, and is meant to represent a pipeline of chaining/redefinitions. This is similar (and can happily be
    used in conjunction with) `pipe` in pandas. In Pyspark this is akin to the common operation of redefining a dataframe
    with new columns. While it is generally reasonable to contain these constructs within a node's function,
    you should consider `pipe` for any of the following reasons:

    1.  You want the transformations to display as nodes in the DAG, with the possibility of storing or visualizing
    the result
    2. You want to pull in functions from an external repository, and build the DAG a little more procedurally
    3. You want to use the same function multiple times, but with different parameters -- while `@does`/`@parameterize` can
    do this, this presents an easier way to do this, especially in a chain.

    To demonstrate the rules for chaining nodes, we'll be using the following example. This is
    using primitives to demonstrate, but as hamilton is just functions of any python objects, this works perfectly with
    dataframes, series, etc...


    .. code-block:: python
        :name: Simple @pipe example

        from hamilton.function_modifiers import step, pipe, value, source

        def _add_one(x: int) -> int:
            return x + 1

        def _sum(x: int, y: int) -> int:
            return x + y

        def _multiply(x: int, y: int, z: int=10) -> int:
            return x * y * z

        @pipe(
            step(_add_one),
            step(_multiply, y=2),
            step(_sum, y=value(3)),
            step(_multiply, y=source("upstream_node_to_multiply")),
        )
        def final_result(upstream_int: int) -> int:
            return upstream_int

    .. code-block:: python
        :name: Equivalent example with no @pipe, nested

        upstream_int = ... # result from upstream
        upstream_node_to_multiply = ... # result from upstream

        output = final_result(
            _multiply(
                _sum(
                    _multiply(
                        _add_one(upstream_int),
                        y=2
                    ),
                    y=3
                ),
                y=upstream_node_to_multiply
            )
        )

    .. code-block:: python
        :name: Equivalent example with no @pipe, procedural

        upstream_int = ... # result from upstream
        upstream_node_to_multiply = ... # result from upstream

        one_added = _add_one(upstream_int)
        multiplied = _multiply(one_added, y=2)
        summed = _sum(multiplied, y=3)
        multiplied_again = _multiply(summed, y=upstream_node_to_multiply)
        output = final_result(multiplied_again)


    Note that functions must have no position-only arguments (this is rare in python, but hamilton does not handle these).
    This basically means that the functions must be defined similarly to `def fn(x, y, z=10)` and not `def fn(x, y, /, z=10)`.
    In fact, all arguments must be named and "kwarg-friendly", meaning that the function can happily be called with `**kwargs`,
    where kwargs are some set of resolved upstream values. So, no `*args` are allowed, and `**kwargs` (variable keyword-only) are not
    permitted. Note that this is not a design limitation, rather an implementation detail -- if you feel like you need this, please
    reach out.

    Furthermore, the function should be typed, as a Hamilton function would be.

    One has two ways to tune the shape/implementation of the subsequent nodes:

    1. `when`/`when_not`/`when_in`/`when_not_in` -- these are used to filter the application of the function. This is valuable to reflect
        if/else conditions in the structure of the DAG, pulling it out of functions, rather than buried within the logic itself. It is functionally
        equivalent to `@config.when`.

        For instance, if you want to include a function in the chain only when a config parameter is set to a certain value, you can do:

        .. code-block:: python

            @pipe(
                step(_add_one).when(foo="bar"),
                step(_add_two, y=source("other_node_to_add").when(foo="baz"),
            )
            def final_result(upstream_int: int) -> int:
                return upstream_int

        This will only apply the first function when the config parameter `foo` is set to `bar`, and the second when it is set to `baz`.

    2. `named` -- this is used to name the node. This is useful if you want to refer to intermediate results. If this is left out,
        hamilton will automatically name the functions in a globally unique manner. The names of
        these functions will not necessarily be stable/guaranteed by the API, so if you want to refer to them, you should use `named`.
        The default namespace will always be the name of the decorated function (which will be the last node in the chain).

        `named` takes in two parameters -- required is the `name` -- this will assign the nodes with a single name and *no* global namespace.
        For instance:

        .. code-block:: python

            @pipe(
                step(_add_one).named("a"),
                step(_add_two, y=source("upstream_node")).named("b"),
            )
            def final_result(upstream_int: int) -> int:
                return upstream_int

        The above will create two nodes, `a` and `b`. `a` will be the result of `_add_one`, and `b` will be the result of `_add_two`.
        `final_result` will then be called with the output of `b`. Note that, if these are part of a namespaced operation (a subdag, in particular),
        they *will* get the same namespace as the subdag.

        The second parameter is `namespace`. This is used to specify a namespace for the node. This is useful if you want
        to either (a) ensure that the nodes are namespaced but share a common one to avoid name clashes (usual case), or (b)
        if you want a custom namespace (unusual case). To indicate a custom namespace, one need simply pass in a string.

        To indicate that a node should share a namespace with the rest of the step(...) operations in a pipe, one can pass in `...` (the ellipsis).

        .. code-block:: python
          :name: Namespaced step


            @pipe(
                step(_add_one).named("a", namespace="foo"), # foo.a
                step(_add_two, y=source("upstream_node")).named("b", namespace=...), # final_result.b
            )
            def final_result(upstream_int: int) -> int:
                return upstream_int

        Note that if you pass a namespace argument to the `pipe` function, it will set the namespace on each step operation.
        This is useful if you want to ensure that all the nodes in a pipe have a common namespace, but you want to rename them.

        .. code-block:: python
            :name: pipe with globally applied namespace

            @pipe(
                step(_add_one).named("a"), # a
                step(_add_two, y=source("upstream_node")).named("b"), # foo.b
                namespace=..., # default -- final_result.a and final_result.b, OR
                namespace=None, # no namespace -- a and b are exposed as that, OR
                namespace="foo", # foo.a and foo.b
            )
            def final_result(upstream_int: int) -> int:
                return upstream_int

        In all likelihood, you should not be using this, and this is only here in case you want to expose a node for
        consumption/output later. Setting the namespace in individual nodes as well as in `pipe` is not yet supported.
    """

    def __init__(
        self,
        *transforms: Applicable,
        namespace: NamespaceType = ...,
        collapse=False,
        _chain=False,
    ):
        """Instantiates a `@pipe` decorator.

        :param transforms: step transformations to be applied, in order
        :param namespace: namespace to apply to all nodes in the pipe. This can be "..." (the default), which resolves to the name of the decorated function, None (which means no namespace), or a string (which means that all nodes will be namespaced with that string). Note that you can either use this *or* namespaces inside pipe()...
        :param collapse: Whether to collapse this into a single node. This is not currently supported.
        :param _chain: Whether to chain the first parameter. This is the only mode that is supported. Furthermore, this is not externally exposed. @flow will make use of this.
        """
        self.transforms = transforms
        self.collapse = collapse
        self.chain = _chain
        self.namespace = namespace

        if self.collapse:
            raise NotImplementedError(
                "Collapsing step() functions as one node is not yet implemented for pipe(). Please reach out if you want this feature."
            )

        if self.chain:
            raise NotImplementedError("@flow() is not yet supported -- this is ")

    def inject_nodes(
        self, params: Dict[str, Type[Type]], config: Dict[str, Any], fn: Callable
    ) -> Tuple[List[node.Node], Dict[str, str]]:
        """Injects nodes into the graph. This creates a node for each pipe() step,
        then reassigns the inputs to pass it in."""
        sig = inspect.signature(fn)
        first_parameter = list(sig.parameters.values())[0].name
        # use the name of the parameter to determine the first node
        # Then wire them all through in order
        # if it resolves, great
        # if not, skip that, pointing to the previous
        # Create a node along the way
        if first_parameter not in params:
            raise base.InvalidDecoratorException(
                f"Function: {fn.__name__} has a first parameter that is not a dependency. "
                f"@pipe requires the parameter names to match the function parameters. "
                f"Thus it might not be compatible with some other decorators"
            )
        current_param = first_parameter
        fn_count = Counter()
        nodes = []
        for applicable in self.transforms:
            if self.namespace is not ...:
                applicable = applicable.namespaced(
                    namespace=self.namespace
                )  # we reassign the global namespace
            if applicable.resolves(config):
                fn_name = applicable.fn.__name__
                postfix = "" if fn_count[fn_name] == 0 else f"_{fn_count[fn_name]}"
                node_name = (
                    applicable.name
                    if applicable.name is not None
                    else f"with{('_' if not fn_name.startswith('_') else '') + fn_name}{postfix}"
                )
                raw_node = node.Node.from_fn(
                    applicable.fn,
                    f"with{('_' if not fn_name.startswith('_') else '') + fn_name}{postfix}",
                )
                node_namespace = applicable.resolve_namespace(fn.__name__)
                raw_node = raw_node.copy_with(namespace=node_namespace, name=node_name)
                # TODO -- validate that the first parameter is the right type/all the same
                fn_count[fn_name] += 1
                upstream_inputs, literal_inputs = applicable.bind_function_args(current_param)
                nodes.append(
                    raw_node.reassign_inputs(
                        input_names=upstream_inputs,
                        input_values=literal_inputs,
                    )
                )
                current_param = raw_node.name
        return nodes, {first_parameter: current_param}  # rename to ensure it all works

    def validate(self, fn: Callable):
        """Validates the the individual steps work together."""
        for applicable in self.transforms:
            applicable.validate(
                chain_first_param=True, allow_custom_namespace=self.namespace is ...
            )
        # TODO -- validate that the types match on the chain (this is de-facto done later)

    def optional_config(self) -> Dict[str, Any]:
        """Declares the optional configuration keys for this decorator.
        These are configuration keys that can be used by the decorator, but are not required.
        Along with these we have *defaults*, which we will use to pass to the config.

        :return: The optional configuration keys with defaults. Note that this will return None
        if we have no idea what they are, which bypasses the configuration filtering we use entirely.
        This is mainly for the legacy API.
        """
        out = {}
        for applicable in self.transforms:
            for resolver in applicable.resolvers:
                out.update(resolver.optional_config)
        return out


# # TODO -- implement flow!
# class flow(pipe):
#     """flow() is a more flexible, power-user version of `pipe`. The rules are largely similar, with a few key differences:
#
#     1. The first parameter is not passed through -- the user is responsible for passing all parameters into a function
#     2. The final function can depend on any of the prior functions -- it will declare those as inputs using the input parameters. These will
#     be seen as inputs, regardless of namespace.
#
#     This means that `flow` can be used to construct *any* DAG -- this is why its a power-user capability. Before we dig into some examples,
#     a quick note on when to use it:
#
#     flow is meant to procedurally specify a subcomponent of the DAG. While Hamilton encourage declarative (not procedural) DAGs, there are
#     certain cases where you may find yourself wanting to more dynamically construct a DAG, for certain subcomponents. This is where
#     `flow` comes in. As we will show later in this doc, this can be very powerful when combined with `resolve` to build configuration-driven
#     DAGs. Again, however, this is meant to be a subset of a declarative DAG -- procedurally defined subdags can help with flexibility, but
#     should not be overused.
#
#     Now, let's get to some examples:
#
#     TODO -- basic example
#
#     TODO -- example with resolve
#
#     TODO -- examples with namespacing
#     """
#
#     def __init__(self, *transforms: Applicable, collapse=False):
#         super(flow, self).__init__(*transforms, collapse=collapse, _chain=False)
