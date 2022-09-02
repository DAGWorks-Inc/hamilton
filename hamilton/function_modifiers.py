import abc
import collections
import dataclasses
import enum
import functools
import inspect
import logging
import typing
from typing import Any, Callable, Collection, Dict, List, Tuple, Type, Union

import pandas as pd
import typing_inspect

from hamilton import function_modifiers_base, models, node, type_utils
from hamilton.data_quality import base as dq_base
from hamilton.data_quality import default_validators
from hamilton.dev_utils import deprecation

logger = logging.getLogger(__name__)

"""
Annotations for modifying the way functions get added to the DAG.
All user-facing annotation classes are lowercase as they're meant to be used
as annotations. They are classes to hold state and subclass common functionality.
"""


class InvalidDecoratorException(Exception):
    pass


def get_default_tags(fn: Callable) -> Dict[str, str]:
    """Function that encapsulates default tags on a function.

    :param fn: the function we want to create default tags for.
    :return: a dictionary with str -> str values representing the default tags.
    """
    module_name = inspect.getmodule(fn).__name__
    return {"module": module_name}


# TODO -- replace this with polymorphism for assigning/grabbing
#  dependencies once we have the computation decided, if needed
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


class parameterize(function_modifiers_base.NodeExpander):
    RESERVED_KWARG = "output_name"

    def __init__(
        self,
        **parametrization: Union[
            Dict[str, ParametrizedDependency], Tuple[Dict[str, ParametrizedDependency], str]
        ],
    ):
        """Creates a parameterize decorator. For example:
            @parameterize(
                replace_no_parameters=({}, 'fn with no parameters replaced'),
                replace_just_upstream_parameter=({'upstream_parameter': source('foo_source')}, 'fn with upstream_parameter set to node foo'),
                replace_just_literal_parameter=({'literal_parameter': value('bar')}, 'fn with parameter literal_parameter set to value bar'),
                replace_both_parameters=({'upstream_parameter': source('foo_source'), 'literal_parameter': value('bar')}, 'fn with both parameters replaced')
            )
            def concat(upstream_parameter: str, literal_parameter: str) -> Any:
                return f'{upstream_parameter}{literal_parameter}'

        :param parametrization: **kwargs with one of two things:
            - a tuple of assignments (consisting of literals/upstream specifications), and docstring
            - just assignments, in which case it parametrizes the existing docstring
        """
        self.parametrization = {
            key: (value[0] if isinstance(value, tuple) else value)
            for key, value in parametrization.items()
        }
        bad_values = []
        for assigned_output, mapping in self.parametrization.items():
            for parameter, value in mapping.items():
                if not isinstance(value, ParametrizedDependency):
                    bad_values.append(value)
        if bad_values:
            raise InvalidDecoratorException(
                f"@parameterize must specify a dependency type -- either source() or value()."
                f"The following are not allowed: {bad_values}."
            )
        self.specified_docstrings = {
            key: value[1] for key, value in parametrization.items() if isinstance(value, tuple)
        }

    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        nodes = []
        for output_node, parametrization_with_optional_docstring in self.parametrization.items():
            if isinstance(
                parametrization_with_optional_docstring, tuple
            ):  # In this case it contains the docstring
                (parametrization,) = parametrization_with_optional_docstring
            else:
                parametrization = parametrization_with_optional_docstring
            docstring = self.format_doc_string(fn.__doc__, output_node)
            upstream_dependencies = {
                parameter: replacement
                for parameter, replacement in parametrization.items()
                if replacement.get_dependency_type() == ParametrizedDependencySource.UPSTREAM
            }
            literal_dependencies = {
                parameter: replacement
                for parameter, replacement in parametrization.items()
                if replacement.get_dependency_type() == ParametrizedDependencySource.LITERAL
            }

            def replacement_function(
                *args,
                upstream_dependencies=upstream_dependencies,
                literal_dependencies=literal_dependencies,
                **kwargs,
            ):
                """This function rewrites what is passed in kwargs to the right kwarg for the function."""
                kwargs = kwargs.copy()
                for dependency, replacement in upstream_dependencies.items():
                    kwargs[dependency] = kwargs.pop(replacement.source)
                for dependency, replacement in literal_dependencies.items():
                    kwargs[dependency] = replacement.value
                return node_.callable(*args, **kwargs)

            new_input_types = {}
            for param, value in node_.input_types.items():
                if param in upstream_dependencies:
                    new_input_types[
                        upstream_dependencies[param].source
                    ] = value  # We replace with the upstream_dependencies
                elif param not in literal_dependencies:
                    new_input_types[
                        param
                    ] = value  # We just use the standard one, nothing is getting replaced

            nodes.append(
                node.Node(
                    name=output_node,
                    typ=node_.type,
                    doc_string=docstring,  # TODO -- change docstring
                    callabl=functools.partial(
                        replacement_function,
                        **{
                            parameter: value.value
                            for parameter, value in literal_dependencies.items()
                        },
                    ),
                    input_types=new_input_types,
                    tags=node_.tags.copy(),
                )
            )
        return nodes

    def validate(self, fn: Callable):
        signature = inspect.signature(fn)
        func_param_names = set(signature.parameters.keys())
        try:
            for output_name, mappings in self.parametrization.items():
                # TODO -- separate out into the two dependency-types
                self.format_doc_string(fn.__doc__, output_name)
        except KeyError as e:
            raise InvalidDecoratorException(
                f"Function docstring templating is incorrect. "
                f"Please fix up the docstring {fn.__module__}.{fn.__name__}."
            ) from e

        if self.RESERVED_KWARG in func_param_names:
            raise InvalidDecoratorException(
                f"Error function {fn.__module__}.{fn.__name__} cannot have `{self.RESERVED_KWARG}` "
                f"as a parameter it is reserved."
            )
        missing_parameters = set()
        for mapping in self.parametrization.values():
            for param_to_replace in mapping:
                if param_to_replace not in func_param_names:
                    missing_parameters.add(param_to_replace)
        if missing_parameters:
            raise InvalidDecoratorException(
                f"Parametrization is invalid: the following parameters don't appear in the function itself: {', '.join(missing_parameters)}"
            )

    def format_doc_string(self, doc: str, output_name: str) -> str:
        """Helper function to format a function documentation string.

        :param doc: the string template to format
        :param output_name: the output name of the function
        :param params: the parameter mappings
        :return: formatted string
        :raises: KeyError if there is a template variable missing from the parameter mapping.
        """

        class IdentityDict(dict):
            # quick hack to allow for formatting of missing parameters
            def __missing__(self, key):
                return key

        if output_name in self.specified_docstrings:
            return self.specified_docstrings[output_name]
        if doc is None:
            return None
        parametrization = self.parametrization[output_name]
        upstream_dependencies = {
            parameter: replacement.source
            for parameter, replacement in parametrization.items()
            if replacement.get_dependency_type() == ParametrizedDependencySource.UPSTREAM
        }
        literal_dependencies = {
            parameter: replacement.value
            for parameter, replacement in parametrization.items()
            if replacement.get_dependency_type() == ParametrizedDependencySource.LITERAL
        }
        return doc.format_map(
            IdentityDict(
                **{self.RESERVED_KWARG: output_name},
                **{**upstream_dependencies, **literal_dependencies},
            )
        )


class parameterize_values(parameterize):
    def __init__(self, parameter: str, assigned_output: Dict[Tuple[str, str], Any]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to a function in which the parameter value is replaced by that *specific value*.

        :param parameter: Parameter to expand on.
        :param assigned_output: A map of tuple of [parameter names, documentation] to values
        """
        for node_ in assigned_output.keys():
            if not isinstance(node_, Tuple):
                raise InvalidDecoratorException(
                    f"assigned_output key is incorrect: {node_}. The parameterized decorator needs a dict of "
                    "[name, doc string] -> value to function."
                )
        super(parameterize_values, self).__init__(
            **{
                output: ({parameter: value(literal_value)}, documentation)
                for (output, documentation), literal_value in assigned_output.items()
            }
        )


@deprecation.deprecated(
    warn_starting=(1, 10, 0),
    fail_starting=(2, 0, 0),
    use_this=parameterize_values,
    explanation="We now support three parametrize decorators. @parameterize, @parameterize_values, and @parameterize_inputs",
    migration_guide="https://github.com/stitchfix/hamilton/blob/main/decorators.md#migrating-parameterized",
)
class parametrized(parameterize_values):
    pass


class parameterize_sources(parameterize):
    def __init__(self, **parameterization: Dict[str, Dict[str, str]]):
        """Constructor for a modifier that expands a single function into n, each of which corresponds to replacing
        some subset of the specified parameters with specific upstream nodes.

        Note this decorator and `@parametrized_input` are similar, except this one allows multiple
        parameters to be mapped to multiple function arguments (and it fixes the spelling mistake).

        `parameterized_sources` allows you keep your code DRY by reusing the same function but replace the inputs
        to create multiple corresponding distinct outputs. We see here that `parameterized_inputs` allows you to keep
        your code DRY by reusing the same function to create multiple distinct outputs. The key word arguments passed
        have to have the following structure:
            > OUTPUT_NAME = Mapping of function argument to input that should go into it.
        The documentation for the output is taken from the function. The documentation string can be templatized with
        the parameter names of the function and the reserved value `output_name` - those will be replaced with the
        corresponding values from the parameterization.

        :param **parameterization: kwargs of output name to dict of parameter mappings.
        """
        self.parametrization = parameterization
        if not parameterization:
            raise ValueError("Cannot pass empty/None dictionary to parameterize_sources")
        for output, mappings in parameterization.items():
            if not mappings:
                raise ValueError(
                    f"Error, {output} has a none/empty dictionary mapping. Please fill it."
                )
        super(parameterize_sources, self).__init__(
            **{
                output: {
                    parameter: source(upstream_node) for parameter, upstream_node in mapping.items()
                }
                for output, mapping in parameterization.items()
            }
        )


@deprecation.deprecated(
    warn_starting=(1, 10, 0),
    fail_starting=(2, 0, 0),
    use_this=parameterize_sources,
    explanation="We now support three parametrize decorators. @parameterize, @parameterize_values, and @parameterize_inputs",
    migration_guide="https://github.com/stitchfix/hamilton/blob/main/decorators.md#migrating-parameterized",
)
class parametrized_input(parameterize):
    def __init__(self, parameter: str, variable_inputs: Dict[str, Tuple[str, str]]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to the specified parameter replaced by a *specific input column*.

        Note this decorator and `@parametrized` are quite similar, except that the input here is another DAG node,
        i.e. column, rather than some specific value.

        The `parameterized_input` allows you keep your code DRY by reusing the same function but replace the inputs
        to create multiple corresponding distinct outputs. The _parameter_ key word argument has to match one of the
        arguments in the function. The rest of the arguments are pulled from items inside the DAG.
        The _assigned_inputs_ key word argument takes in a
        dictionary of input_column -> tuple(Output Name, Documentation string).

        :param parameter: Parameter to expand on.
        :param variable_inputs: A map of tuple of [parameter names, documentation] to values
        """
        for value in variable_inputs.values():
            if not isinstance(value, Tuple):
                raise InvalidDecoratorException(
                    f"assigned_output key is incorrect: {node}. The parameterized decorator needs a dict of "
                    "input column -> [name, description] to function."
                )
        super(parametrized_input, self).__init__(
            **{
                output: ({parameter: source(value)}, documentation)
                for value, (output, documentation) in variable_inputs.items()
            }
        )


@deprecation.deprecated(
    warn_starting=(1, 10, 0),
    fail_starting=(2, 0, 0),
    use_this=parameterize_sources,
    explanation="We now support three parametrize decorators. @parameterize, @parameterize_values, and @parameterize_inputs",
    migration_guide="https://github.com/stitchfix/hamilton/blob/main/decorators.md#migrating-parameterized",
)
class parameterized_inputs(parameterize_sources):
    pass


class extract_columns(function_modifiers_base.NodeExpander):
    def __init__(self, *columns: Union[Tuple[str, str], str], fill_with: Any = None):
        """Constructor for a modifier that expands a single function into the following nodes:
        - n functions, each of which take in the original dataframe and output a specific column
        - 1 function that outputs the original dataframe

        :param columns: Columns to extract, that can be a list of tuples of (name, documentation) or just names.
        :param fill_with: If you want to extract a column that doesn't exist, do you want to fill it with a default value?
        Or do you want to error out? Leave empty/None to error out, set fill_value to dynamically create a column.
        """
        if not columns:
            raise InvalidDecoratorException(
                "Error empty arguments passed to extract_columns decorator."
            )
        elif isinstance(columns[0], list):
            raise InvalidDecoratorException(
                "Error list passed in. Please `*` in front of it to expand it."
            )
        self.columns = columns
        self.fill_with = fill_with

    def validate(self, fn: Callable):
        """A function is invalid if it does not output a dataframe.

        :param fn: Function to validate.
        :raises: InvalidDecoratorException If the function does not output a Dataframe
        """
        output_type = inspect.signature(fn).return_annotation
        if not issubclass(output_type, pd.DataFrame):
            raise InvalidDecoratorException(
                f"For extracting columns, output type must be pandas dataframe, not: {output_type}"
            )

    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """For each column to extract, output a node that extracts that column. Also, output the original dataframe
        generator.

        :param config:
        :param fn: Function to extract columns from. Must output a dataframe.
        :return: A collection of nodes --
                one for the original dataframe generator, and another for each column to extract.
        """
        fn = node_.callable
        base_doc = node_.documentation

        @functools.wraps(fn)
        def df_generator(*args, **kwargs):
            df_generated = fn(*args, **kwargs)
            if self.fill_with is not None:
                for col in self.columns:
                    if col not in df_generated:
                        df_generated[col] = self.fill_with
            return df_generated

        output_nodes = [
            node.Node(
                node_.name,
                typ=pd.DataFrame,
                doc_string=base_doc,
                callabl=df_generator,
                tags=node_.tags.copy(),
            )
        ]

        for column in self.columns:
            doc_string = base_doc  # default doc string of base function.
            if isinstance(column, Tuple):  # Expand tuple into constituents
                column, doc_string = column

            def extractor_fn(
                column_to_extract: str = column, **kwargs
            ) -> pd.Series:  # avoiding problems with closures
                df = kwargs[node_.name]
                if column_to_extract not in df:
                    raise InvalidDecoratorException(
                        f"No such column: {column_to_extract} produced by {node_.name}. "
                        f"It only produced {str(df.columns)}"
                    )
                return kwargs[node_.name][column_to_extract]

            output_nodes.append(
                node.Node(
                    column,
                    pd.Series,
                    doc_string,
                    extractor_fn,
                    input_types={node_.name: pd.DataFrame},
                    tags=node_.tags.copy(),
                )
            )
        return output_nodes


class extract_fields(function_modifiers_base.NodeExpander):
    """Extracts fields from a dictionary of output."""

    def __init__(self, fields: dict, fill_with: Any = None):
        """Constructor for a modifier that expands a single function into the following nodes:
        - n functions, each of which take in the original dict and output a specific field
        - 1 function that outputs the original dict

        :param fields: Fields to extract. A dict of 'field_name' -> 'field_type'.
        :param fill_with: If you want to extract a field that doesn't exist, do you want to fill it with a default value?
        Or do you want to error out? Leave empty/None to error out, set fill_value to dynamically create a field value.
        """
        if not fields:
            raise InvalidDecoratorException(
                "Error an empty dict, or no dict, passed to extract_fields decorator."
            )
        elif not isinstance(fields, dict):
            raise InvalidDecoratorException(f"Error, please pass in a dict, not {type(fields)}")
        else:
            errors = []
            for field, field_type in fields.items():
                if not isinstance(field, str):
                    errors.append(f"{field} is not a string. All keys must be strings.")
                if not isinstance(field_type, type):
                    errors.append(
                        f"{field} does not declare a type. Instead it passes {field_type}."
                    )

            if errors:
                raise InvalidDecoratorException(
                    f"Error, found these {errors}. " f"Please pass in a dict of string to types. "
                )
        self.fields = fields
        self.fill_with = fill_with

    def validate(self, fn: Callable):
        """A function is invalid if it is not annotated with a dict or typing.Dict return type.

        :param fn: Function to validate.
        :raises: InvalidDecoratorException If the function is not annotated with a dict or typing.Dict type as output.
        """
        output_type = inspect.signature(fn).return_annotation
        if typing_inspect.is_generic_type(output_type):
            base = typing_inspect.get_origin(output_type)
            if (
                base == dict or base == typing.Dict
            ):  # different python versions return different things 3.7+ vs 3.6.
                pass
            else:
                raise InvalidDecoratorException(
                    f"For extracting fields, output type must be a dict or typing.Dict, not: {output_type}"
                )
        elif output_type == dict:
            pass
        else:
            raise InvalidDecoratorException(
                f"For extracting fields, output type must be a dict or typing.Dict, not: {output_type}"
            )

    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        """For each field to extract, output a node that extracts that field. Also, output the original TypedDict
        generator.

        :param node_:
        :param config:
        :param fn: Function to extract columns from. Must output a dataframe.
        :return: A collection of nodes --
                one for the original dataframe generator, and another for each column to extract.
        """
        fn = node_.callable
        base_doc = node_.documentation

        @functools.wraps(fn)
        def dict_generator(*args, **kwargs):
            dict_generated = fn(*args, **kwargs)
            if self.fill_with is not None:
                for field in self.fields:
                    if field not in dict_generated:
                        dict_generated[field] = self.fill_with
            return dict_generated

        output_nodes = [
            node.Node(
                node_.name,
                typ=dict,
                doc_string=base_doc,
                callabl=dict_generator,
                tags=node_.tags.copy(),
            )
        ]

        for field, field_type in self.fields.items():
            doc_string = base_doc  # default doc string of base function.

            def extractor_fn(
                field_to_extract: str = field, **kwargs
            ) -> field_type:  # avoiding problems with closures
                dt = kwargs[node_.name]
                if field_to_extract not in dt:
                    raise InvalidDecoratorException(
                        f"No such field: {field_to_extract} produced by {node_.name}. "
                        f"It only produced {list(dt.keys())}"
                    )
                return kwargs[node_.name][field_to_extract]

            output_nodes.append(
                node.Node(
                    field,
                    field_type,
                    doc_string,
                    extractor_fn,
                    input_types={node_.name: dict},
                    tags=node_.tags.copy(),
                )
            )
        return output_nodes


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
        raise InvalidDecoratorException(
            f"Function: {fn.__name__} is not empty. Must have only one line that "
            f'consists of "pass"'
        )


class does(function_modifiers_base.NodeCreator):
    def __init__(self, replacing_function: Callable, **argument_mapping: Union[str, List[str]]):
        """Constructor for a modifier that replaces the annotated functions functionality with something else.
        Right now this has a very strict validation requirements to make compliance with the framework easy.
        :param replacing_function: The function to replace the original function with
        :param argument_mapping: A mapping of argument name in the replacing function to argument name in the decorating function
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
            raise InvalidDecoratorException(
                f"Decorated function for @does (and really, all of hamilton), "
                f"can only consist of keyword-friendly arguments. "
                f"The following parameters for {og_function.__name__} are not keyword-friendly: {invalid_fn_parameters}"
            )
        if not does.test_function_signatures_compatible(
            inspect.signature(og_function), inspect.signature(replacing_function), argument_mapping
        ):
            raise InvalidDecoratorException(
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

    def generate_node(self, fn: Callable, config) -> node.Node:
        """Returns one node which has the replaced functionality
        :param fn: Function to decorate
        :param config: Configuration (not used in this)
        :return: A node with the function in `@does` injected,
        and the same parameters/types as the original function.
        """

        def replacing_function(__fn=fn, **kwargs):
            final_kwarg_values = {
                key: param_spec.default
                for key, param_spec in inspect.signature(fn).parameters.items()
                if param_spec.default != inspect.Parameter.empty
            }
            final_kwarg_values.update(kwargs)
            final_kwarg_values = does.map_kwargs(final_kwarg_values, self.argument_mapping)
            return self.replacing_function(**final_kwarg_values)

        return node.Node.from_fn(fn).copy_with(callabl=replacing_function)


class dynamic_transform(function_modifiers_base.NodeCreator):
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
        if not issubclass(signature.return_annotation, pd.Series):
            raise InvalidDecoratorException(
                "Models must declare their return type as a pandas Series"
            )
        if len(signature.parameters) > 0:
            raise InvalidDecoratorException(
                "Models must have no parameters -- all are passed in through the config"
            )

    def generate_node(self, fn: Callable, config: Dict[str, Any] = None) -> node.Node:
        if self.config_param not in config:
            raise InvalidDecoratorException(
                f"Configuration has no parameter: {self.config_param}. Did you define it? If so did you spell it right?"
            )
        fn_name = fn.__name__
        transform = self.transform_cls(
            config[self.config_param], fn_name, **self.extra_transform_params
        )
        return node.Node(
            name=fn_name,
            typ=inspect.signature(fn).return_annotation,
            doc_string=fn.__doc__,
            callabl=transform.compute,
            input_types={dep: pd.Series for dep in transform.get_dependents()},
            tags=get_default_tags(fn),
        )


class model(dynamic_transform):
    """Model, same as a dynamic transform"""

    def __init__(self, model_cls, config_param: str, **extra_model_params):
        super(model, self).__init__(
            transform_cls=model_cls, config_param=config_param, **extra_model_params
        )


class config(function_modifiers_base.NodeResolver):
    """Decorator class that resolves a node's function based on  some configuration variable
    Currently, functions that exist in all configurations have to be disjoint.
    E.G. for every config.when(), you can have a config.when_not() that filters the opposite.
    That said, you can have functions that *only* exist in certain configurations without worrying about it.
    """

    def __init__(self, resolves: Callable[[Dict[str, Any]], bool], target_name: str = None):
        self.does_resolve = resolves
        self.target_name = target_name

    def _get_function_name(self, fn: Callable) -> str:
        if self.target_name is not None:
            return self.target_name
        return function_modifiers_base.sanitize_function_name(fn.__name__)

    def resolve(self, fn, configuration: Dict[str, Any]) -> Callable:
        if not self.does_resolve(configuration):
            return None
        fn.__name__ = self._get_function_name(fn)  # TODO -- copy function to not mutate it
        return fn

    def validate(self, fn):
        if fn.__name__.endswith("__"):
            raise InvalidDecoratorException(
                "Config will always use the portion of the function name before the last __. For example, signups__v2 will map to signups, whereas"
            )

    @staticmethod
    def when(name=None, **key_value_pairs) -> "config":
        """Yields a decorator that resolves the function if all keys in the config are equal to the corresponding value

        :param key_value_pairs: Keys and corresponding values to look up in the config
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(value == configuration.get(key) for key, value in key_value_pairs.items())

        return config(resolves, target_name=name)

    @staticmethod
    def when_not(name=None, **key_value_pairs: Any) -> "config":
        """Yields a decorator that resolves the function if none keys in the config are equal to the corresponding value

        :param key_value_pairs: Keys and corresponding values to look up in the config
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(value != configuration.get(key) for key, value in key_value_pairs.items())

        return config(resolves, target_name=name)

    @staticmethod
    def when_in(name=None, **key_value_group_pairs: Collection[Any]) -> "config":
        """Yields a decorator that resolves the function if all of the keys are equal to one of items in the list of values.

        :param key_value_group_pairs: pairs of key-value mappings where the value is a list of possible values
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(
                configuration.get(key) in value for key, value in key_value_group_pairs.items()
            )

        return config(resolves, target_name=name)

    @staticmethod
    def when_not_in(**key_value_group_pairs: Collection[Any]) -> "config":
        """Yields a decorator that resolves the function only if none of the keys are in the list of values.

        :param key_value_group_pairs: pairs of key-value mappings where the value is a list of possible values
        :return: a configuration decorator

        :Example:

        @config.when_not_in(business_line=["mens","kids"], region=["uk"])
        def LEAD_LOG_BASS_MODEL_TIMES_TREND(TREND_BSTS_WOMENS_ACQUISITIONS: pd.Series,
                                    LEAD_LOG_BASS_MODEL_SIGNUPS_NON_REFERRAL: pd.Series) -> pd.Series:

        above will resolve for config has {"business_line": "womens", "region": "us"},
        but not for configs that have {"business_line": "mens", "region": "us"}, {"business_line": "kids", "region": "us"},
        or {"region": "uk"}

        .. seealso:: when_not
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(
                configuration.get(key) not in value for key, value in key_value_group_pairs.items()
            )

        return config(resolves)


class tag(function_modifiers_base.NodeDecorator):
    """Decorator class that adds a tag to a node. Tags take the form of key/value pairings.
    Tags can have dots to specify namespaces (keys with dots), but this is usually reserved for special cases
    (E.G. subdecorators) that utilize them. Usually one will pass in tags as kwargs, so we expect tags to
    be un-namespaced in most uses.

    That is using:
    > @tag(my_tag='tag_value')
    > def my_function(...) -> ...:
    is un-namespaced because you cannot put a `.` in the keyword part (the part before the '=').

    But using:
    > @tag(**{'my.tag': 'tag_value'})
    > def my_function(...) -> ...:
    allows you to add dots that allow you to namespace your tags.

    Currently, tag values are restricted to allowing strings only, although we may consider changing the in the future
    (E.G. thinking of lists).

    Hamilton also reserves the right to change the following:
    * adding purely positional arguments
    * not allowing users to use a certain set of top-level prefixes (E.G. any tag where the top level is one of the
      values in RESERVED_TAG_PREFIX).

    Example usage:
    > @tag(foo='bar', a_tag_key='a_tag_value', **{'namespace.tag_key': 'tag_value'})
    > def my_function(...) -> ...:
    >   ...
    """

    RESERVED_TAG_NAMESPACES = [
        "hamilton",
        "data_quality",
        "gdpr",
        "ccpa",
        "dag",
        "module",
    ]  # Anything that starts with any of these is banned, the framework reserves the right to manage it

    def __init__(self, **tags: str):
        """Constructor for adding tag annotations to a function.

        :param tags: the keys are always going to be strings, so the type annotation here means the values are strings.
            Implicitly this is `Dict[str, str]` but the PEP guideline is to only annotate it with `str`.
        """
        self.tags = tags

    def decorate_node(self, node_: node.Node) -> node.Node:
        """Decorates the nodes produced by this with the specified tags

        :param node_: Node to decorate
        :return: Copy of the node, with tags assigned
        """
        unioned_tags = self.tags.copy()
        unioned_tags.update(node_.tags)
        return node.Node(
            name=node_.name,
            typ=node_.type,
            doc_string=node_.documentation,
            callabl=node_.callable,
            node_source=node_.node_source,
            input_types=node_.input_types,
            tags=unioned_tags,
        )

    @staticmethod
    def _key_allowed(key: str) -> bool:
        """Validates that a tag key is allowed. Rules are:
        1. It must not be empty
        2. It can have dots, which specify a hierarchy of order
        3. All components, when split by dots, must be valid python identifiers
        4. It cannot utilize a reserved namespace

        :param key: The key to validate
        :return: True if it is valid, False if not
        """
        key_components = key.split(".")
        if len(key_components) == 0:
            # empty string...
            return False
        if key_components[0] in tag.RESERVED_TAG_NAMESPACES:
            # Reserved prefixes
            return False
        for key in key_components:
            if not key.isidentifier():
                return False
        return True

    @staticmethod
    def _value_allowed(value: Any) -> bool:
        """Validates that a tag value is allowed. Rules are only that it must be a string.

        :param value: Value to validate
        :return: True if it is valid, False otherwise
        """
        if not isinstance(value, str):
            return False
        return True

    def validate(self, fn: Callable):
        """Validates the decorator. In this case that the set of tags produced is final.

        :param fn: Function that the decorator is called on.
        :raises ValueError: if the specified tags contains invalid ones
        """
        bad_tags = set()
        for key, value in self.tags.items():
            if (not tag._key_allowed(key)) or (not tag._value_allowed(value)):
                bad_tags.add((key, value))
        if bad_tags:
            bad_tags_formatted = ",".join([f"{key}={value}" for key, value in bad_tags])
            raise InvalidDecoratorException(
                f"The following tags are invalid as tags: {bad_tags_formatted} "
                "Tag keys can be split by ., to represent a hierarchy, "
                "but each element of the hierarchy must be a valid python identifier. "
                "Paths components also cannot be empty. "
                "The value can be anything. Note that the following top-level prefixes are "
                f"reserved as well: {self.RESERVED_TAG_NAMESPACES}"
            )


# These are part of the publicly exposed API -- do not change them
# Tests will catch it if you do!
IS_DATA_VALIDATOR_TAG = "hamilton.data_quality.contains_dq_results"
DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG = "hamilton.data_quality.source_node"


class BaseDataValidationDecorator(function_modifiers_base.NodeTransformer):
    @abc.abstractmethod
    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        """Returns a list of validators used to transform the nodes.

        :param node_to_validate: Nodes to which the output of the validator will apply
        :return: A list of validators to apply to the node.
        """
        pass

    def transform_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        raw_node = node.Node(
            name=node_.name
            + "_raw",  # TODO -- make this unique -- this will break with multiple validation decorators, which we *don't* want
            typ=node_.type,
            doc_string=node_.documentation,
            callabl=node_.callable,
            node_source=node_.node_source,
            input_types=node_.input_types,
            tags=node_.tags,
        )
        validators = self.get_validators(node_)
        validator_nodes = []
        validator_name_map = {}
        for validator in validators:

            def validation_function(validator_to_call: dq_base.DataValidator = validator, **kwargs):
                result = list(kwargs.values())[0]  # This should just have one kwarg
                return validator_to_call.validate(result)

            validator_node_name = node_.name + "_" + validator.name()
            validator_node = node.Node(
                name=validator_node_name,  # TODO -- determine a good approach towards naming this
                typ=dq_base.ValidationResult,
                doc_string=validator.description(),
                callabl=validation_function,
                node_source=node.NodeSource.STANDARD,
                input_types={raw_node.name: (node_.type, node.DependencyType.REQUIRED)},
                tags={
                    **node_.tags,
                    **{
                        function_modifiers_base.NodeTransformer.NON_FINAL_TAG: True,  # This is not to be used as a subdag later on
                        IS_DATA_VALIDATOR_TAG: True,
                        DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG: node_.name,
                    },
                },
            )
            validator_name_map[validator_node_name] = validator
            validator_nodes.append(validator_node)

        def final_node_callable(
            validator_nodes=validator_nodes, validator_name_map=validator_name_map, **kwargs
        ):
            """Callable for the final node. First calls the action on every node, then

            :param validator_nodes:
            :param validator_name_map:
            :param kwargs:
            :return: returns the original node output
            """
            failures = []
            for validator_node in validator_nodes:
                validator: dq_base.DataValidator = validator_name_map[validator_node.name]
                validation_result: dq_base.ValidationResult = kwargs[validator_node.name]
                if validator.importance == dq_base.DataValidationLevel.WARN:
                    dq_base.act_warn(node_.name, validation_result, validator)
                else:
                    failures.append((validation_result, validator))
            dq_base.act_fail_bulk(node_.name, failures)
            return kwargs[raw_node.name]

        final_node = node.Node(
            name=node_.name,
            typ=node_.type,
            doc_string=node_.documentation,
            callabl=final_node_callable,
            node_source=node_.node_source,
            input_types={
                raw_node.name: (node_.type, node.DependencyType.REQUIRED),
                **{
                    validator_node.name: (validator_node.type, node.DependencyType.REQUIRED)
                    for validator_node in validator_nodes
                },
            },
            tags=node_.tags,
        )
        return [*validator_nodes, final_node, raw_node]

    def validate(self, fn: Callable):
        pass


class check_output_custom(BaseDataValidationDecorator):
    def __init__(self, *validators: dq_base.DataValidator):
        """Creates a check_output_custom decorator. This allows
        passing of custom validators that implement the DataValidator interface.

        :param validator: Validator to use.
        """
        self.validators = list(validators)

    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        return self.validators


class check_output(BaseDataValidationDecorator):
    def get_validators(self, node_to_validate: node.Node) -> List[dq_base.DataValidator]:
        return default_validators.resolve_default_validators(
            node_to_validate.type,
            importance=self.importance,
            available_validators=self.default_decorator_candidates,
            **self.default_validator_kwargs,
        )

    def __init__(
        self,
        importance: str = dq_base.DataValidationLevel.WARN.value,
        default_decorator_candidates: Type[dq_base.BaseDefaultValidator] = None,
        **default_validator_kwargs: Any,
    ):
        """Creates the check_output validator. This constructs the default validator class.
        Note that this creates a whole set of default validators
        TODO -- enable construction of custom validators using check_output.custom(*validators)

        :param importance: For the default validator, how important is it that this passes.
        :param validator_kwargs: keyword arguments to be passed to the validator
        """
        self.importance = importance
        self.default_validator_kwargs = default_validator_kwargs
        self.default_decorator_candidates = default_decorator_candidates
        # We need to wait until we actually have the function in order to construct the validators
        # So, we'll just store the constructor arguments for now and check it in validation

    @staticmethod
    def _validate_constructor_args(
        *validator: dq_base.DataValidator, importance: str = None, **default_validator_kwargs: Any
    ):
        if len(validator) != 0:
            if importance is not None or len(default_validator_kwargs) > 0:
                raise ValueError(
                    "Can provide *either* a list of custom validators or arguments for the default validator. "
                    "Instead received both."
                )
        else:
            if importance is None:
                raise ValueError("Must supply an importance level if using the default validator.")

    def validate(self, fn: Callable):
        """Validates that the check_output node works on the function on which it was called

        :param fn: Function to validate
        :raises: InvalidDecoratorException if the decorator is not valid for the function's return type
        """
        pass
