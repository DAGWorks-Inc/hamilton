import functools
import inspect
from typing import Any, Callable, Collection, Dict, Tuple, Union

import pandas as pd
import typing_inspect

from hamilton import node
from hamilton.dev_utils import deprecation
from hamilton.function_modifiers import base
from hamilton.function_modifiers.dependencies import (
    ParametrizedDependency,
    ParametrizedDependencySource,
    source,
    value,
)

"""Decorators that allow for les dry code by expanding one node into many"""


class parameterize(base.NodeExpander):
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
            for parameter, val in mapping.items():
                if not isinstance(val, ParametrizedDependency):
                    bad_values.append(val)
        if bad_values:
            raise base.InvalidDecoratorException(
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
            for param, val in node_.input_types.items():
                if param in upstream_dependencies:
                    new_input_types[
                        upstream_dependencies[param].source
                    ] = val  # We replace with the upstream_dependencies
                elif param not in literal_dependencies:
                    new_input_types[
                        param
                    ] = val  # We just use the standard one, nothing is getting replaced

            nodes.append(
                node.Node(
                    name=output_node,
                    typ=node_.type,
                    doc_string=docstring,  # TODO -- change docstring
                    callabl=functools.partial(
                        replacement_function,
                        **{parameter: val.value for parameter, val in literal_dependencies.items()},
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
            raise base.InvalidDecoratorException(
                f"Function docstring templating is incorrect. "
                f"Please fix up the docstring {fn.__module__}.{fn.__name__}."
            ) from e

        if self.RESERVED_KWARG in func_param_names:
            raise base.InvalidDecoratorException(
                f"Error function {fn.__module__}.{fn.__name__} cannot have `{self.RESERVED_KWARG}` "
                f"as a parameter it is reserved."
            )
        missing_parameters = set()
        for mapping in self.parametrization.values():
            for param_to_replace in mapping:
                if param_to_replace not in func_param_names:
                    missing_parameters.add(param_to_replace)
        if missing_parameters:
            raise base.InvalidDecoratorException(
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
                raise base.InvalidDecoratorException(
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
        for val in variable_inputs.values():
            if not isinstance(val, Tuple):
                raise base.InvalidDecoratorException(
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


class extract_columns(base.NodeExpander):
    def __init__(self, *columns: Union[Tuple[str, str], str], fill_with: Any = None):
        """Constructor for a modifier that expands a single function into the following nodes:
        - n functions, each of which take in the original dataframe and output a specific column
        - 1 function that outputs the original dataframe

        :param columns: Columns to extract, that can be a list of tuples of (name, documentation) or just names.
        :param fill_with: If you want to extract a column that doesn't exist, do you want to fill it with a default value?
        Or do you want to error out? Leave empty/None to error out, set fill_value to dynamically create a column.
        """
        if not columns:
            raise base.InvalidDecoratorException(
                "Error empty arguments passed to extract_columns decorator."
            )
        elif isinstance(columns[0], list):
            raise base.InvalidDecoratorException(
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
            raise base.InvalidDecoratorException(
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

        def df_generator(*args, **kwargs) -> pd.DataFrame:
            df_generated = fn(*args, **kwargs)
            if self.fill_with is not None:
                for col in self.columns:
                    if col not in df_generated:
                        df_generated[col] = self.fill_with
            return df_generated

        output_nodes = [node_.copy_with(callabl=df_generator)]

        for column in self.columns:
            doc_string = base_doc  # default doc string of base function.
            if isinstance(column, Tuple):  # Expand tuple into constituents
                column, doc_string = column

            def extractor_fn(
                column_to_extract: str = column, **kwargs
            ) -> pd.Series:  # avoiding problems with closures
                df = kwargs[node_.name]
                if column_to_extract not in df:
                    raise base.InvalidDecoratorException(
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


class extract_fields(base.NodeExpander):
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
            raise base.InvalidDecoratorException(
                "Error an empty dict, or no dict, passed to extract_fields decorator."
            )
        elif not isinstance(fields, dict):
            raise base.InvalidDecoratorException(
                f"Error, please pass in a dict, not {type(fields)}"
            )
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
                raise base.InvalidDecoratorException(
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
            base_type = typing_inspect.get_origin(output_type)
            if (
                base_type == dict or base_type == Dict
            ):  # different python versions return different things 3.7+ vs 3.6.
                pass
            else:
                raise base.InvalidDecoratorException(
                    f"For extracting fields, output type must be a dict or typing.Dict, not: {output_type}"
                )
        elif output_type == dict:
            pass
        else:
            raise base.InvalidDecoratorException(
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

        def dict_generator(*args, **kwargs):
            dict_generated = fn(*args, **kwargs)
            if self.fill_with is not None:
                for field in self.fields:
                    if field not in dict_generated:
                        dict_generated[field] = self.fill_with
            return dict_generated

        output_nodes = [node_.copy_with(callabl=dict_generator)]

        for field, field_type in self.fields.items():
            doc_string = base_doc  # default doc string of base function.

            def extractor_fn(
                field_to_extract: str = field, **kwargs
            ) -> field_type:  # avoiding problems with closures
                dt = kwargs[node_.name]
                if field_to_extract not in dt:
                    raise base.InvalidDecoratorException(
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
