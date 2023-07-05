import inspect
import logging
import typing
from typing import Any, Callable, Dict, List, Type, Union

import pandas as pd

from hamilton import models, node
from hamilton.dev_utils.deprecation import deprecated
from hamilton.function_modifiers import base
from hamilton.function_modifiers.delayed import resolve as delayed_resolve

logger = logging.getLogger(__name__)

"""Decorators that replace a function's execution with specified behavior"""


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
            f'consists of "pass"'
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
            if param_spec.default != inspect.Parameter.empty
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
        og_function: Callable, replacing_function: Callable, argument_mapping: Dict[str, str]
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
            inspect.signature(og_function), inspect.signature(replacing_function), argument_mapping
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
                if param_spec.default != inspect.Parameter.empty
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
    migration_guide="https://hamilton.dagworks.io/reference/decorators/",
)
class dynamic_transform(base.NodeCreator):
    def __init__(
        self, transform_cls: Type[models.BaseModel], config_param: str, **extra_transform_params
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
