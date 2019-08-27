import abc
import functools
import inspect
from typing import Dict, Callable, Collection, Tuple, Union, Any

import pandas as pd

from hamilton import graph

"""
Annotations for modifying the way functions get added to the DAG.
All user-facing annotation classes are lowercase as they're meant to be used
as annotations. They are classes to hold state and subclass common functionality.
"""


class InvalidDecoratorException(Exception):
    pass


class NodeExpander(abc.ABC):
    """Abstract class for nodes that "expand" functions into other nodes."""

    # constant that will be the field modified by this decorator
    GENERATE_NODES = 'generate_nodes'

    @abc.abstractmethod
    def get_nodes(self, fn: Callable, config: Dict[str, Any]) -> Collection[graph.Node]:
        """Given a function, converts it to a series of nodes that it produces.

        :param config:
        :param fn: A function to convert.
        :return: A collection of nodes.
        """
        pass

    @abc.abstractmethod
    def validate(self, fn: Callable):
        """Validates that a function will work with this expander

        :param fn: Function to validate.
        :raises InvalidDecoratorException if this is not a valid function for the annotator
        """
        pass

    def __call__(self, fn: Callable):
        self.validate(fn)
        if hasattr(fn, NodeExpander.GENERATE_NODES):
            raise Exception(f'Only one expander annotation allowed at a time. Function {fn} already has an expander '
                            f'annotation.')
        setattr(fn, NodeExpander.GENERATE_NODES, self.get_nodes)
        return fn


class parametrized(NodeExpander):
    def __init__(self, parameter: str, assigned_output: Dict[Tuple[str, str], str]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to a function in which the parameter value is replaced by that specific value.

        :param parameter: Parameter to expand on.
        :param assigned_output: A map of tuple of [parameter names, documentation] to values
        """
        self.parameter = parameter
        self.assigned_output = assigned_output
        for node in assigned_output.keys():
            if not isinstance(node, Tuple):
                raise InvalidDecoratorException(
                    f'assigned_output key is incorrect: {node}. The parameterized decorator needs a dict of '
                    '[name, doc string] -> value to function.')

    def get_nodes(self, fn: Callable, config) -> Collection[graph.Node]:
        """For each parameter value, loop through, partially curry the function, and output a node.

        :param config:
        :param fn: Function to operate on.
        :return: A collection of nodes, each of which is parametrized.
        """
        nodes = []
        input_types = {param_name: param.annotation for param_name, param in inspect.signature(fn).parameters.items()
                       if param_name != self.parameter}
        for (node_name, node_doc), value in self.assigned_output.items():
            nodes.append(
                graph.Node(
                    node_name,
                    inspect.signature(fn).return_annotation,
                    node_doc,
                    functools.partial(fn, **{self.parameter: value}),
                    input_types=input_types))
        return nodes

    def validate(self, fn: Callable):
        """A function is invalid if it does not have the requested parameter.

        :param fn: Function to validate against this annotation.
        :raises: InvalidDecoratorException If the function does not have the requested parameter
        """
        signature = inspect.signature(fn)
        if self.parameter not in signature.parameters.keys():
            raise InvalidDecoratorException(
                f'Annotation is invalid -- no such parameter {self.parameter} in function {fn}')


class extract_columns(NodeExpander):
    def __init__(self, *columns: Union[Tuple[str, str], str], fill_with: Any = None):
        """Constructor for a modifier that expands a single function into the following nodes:
        - n functions, each of which take in the original dataframe and output a specific column
        - 1 function that outputs the original dataframe

        :param columns: Columns to extract, that can be a list of tuples of (name, documentation) or just names.
        :param fill_with: If you want to extract a column that doesn't exist, do you want to fill it with a default value?
        Or do you want to error out? Leave empty/None to error out, set fill_value to dynamically create a column.
        """
        if not columns:
            raise InvalidDecoratorException('Error empty arguments passed to extract_columns decorator.')
        elif isinstance(columns[0], list):
            raise InvalidDecoratorException('Error list passed in. Please `*` in front of it to expand it.')
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
                f'For extracting columns, output type must be pandas dataframe, not: {output_type}')

    def get_nodes(self, fn: Callable, config: Dict[str, Any]) -> Collection[graph.Node]:
        """For each column to extract, output a node that extracts that column. Also, output the original dataframe
        generator.

        :param config:
        :param fn: Function to extract columns from. Must output a dataframe.
        :return: A collection of nodes --
                one for the original dataframe generator, and another for each column to extract.
        """
        node_name = fn.__name__
        base_doc = fn.__doc__ if fn.__doc__ else ''

        @functools.wraps(fn)
        def df_generator(*args, **kwargs):
            df_generated = fn(*args, **kwargs)
            if self.fill_with is not None:
                for col in self.columns:
                    if col not in df_generated:
                        df_generated[col] = self.fill_with
            return df_generated

        output_nodes = [graph.Node(node_name, typ=pd.DataFrame, doc_string=base_doc, callabl=df_generator)]

        for column in self.columns:
            doc_string = base_doc  # default doc string of base function.
            if isinstance(column, Tuple):  # Expand tuple into constituents
                column, doc_string = column

            def extractor_fn(column_to_extract: str = column, **kwargs) -> pd.Series:  # avoiding problems with closures
                df = kwargs[node_name]
                if column_to_extract not in df:
                    raise InvalidDecoratorException(f'No such column: {column_to_extract} produced by {node_name}')
                return kwargs[node_name][column_to_extract]

            output_nodes.append(
                graph.Node(
                    column,
                    pd.Series,
                    doc_string,
                    extractor_fn,
                    input_types={node_name: pd.DataFrame}))
        return output_nodes


# the following are empty functions that we can compare against to ensure that @does uses an empty function
def _empty_function():
    pass


def _empty_function_with_docstring():
    """Docstring for an empty function"""
    pass


class does(NodeExpander):
    def __init__(self, replacing_function: Callable):
        """
        Constructor for a modifier that replaces the annotated functions functionality with something else.
        Right now this has a very strict validation requirements to make compliance with the framework easy.
        """
        self.replacing_function = replacing_function

    @staticmethod
    def ensure_function_empty(fn: Callable):
        """
        Ensures that a function is empty. This is strict definition -- the function must have only one line (and
        possibly a docstring), and that line must say "pass".
        """
        if fn.__code__.co_code not in {_empty_function.__code__.co_code,
                                       _empty_function_with_docstring.__code__.co_code}:
            raise InvalidDecoratorException(f'Function: {fn.__name__} is not empty. Must have only one line that '
                                            f'consists of "pass"')

    @staticmethod
    def ensure_output_types_match(fn: Callable, todo: Callable):
        """
        Ensures that the output types of two functions match.
        """
        annotation_fn = inspect.signature(fn).return_annotation
        annotation_todo = inspect.signature(todo).return_annotation
        if not issubclass(annotation_todo, annotation_fn):
            raise InvalidDecoratorException(f'Output types: {annotation_fn} and {annotation_todo} are not compatible')

    @staticmethod
    def ensure_function_kwarg_only(fn: Callable):
        """
        Ensures that a function is kwarg only. Meaning that it only has one parameter similar to **kwargs.
        """
        parameters = inspect.signature(fn).parameters
        if len(parameters) > 1:
            raise InvalidDecoratorException('Too many parameters -- for now @does can only use **kwarg functions. '
                                            f'Found params: {parameters}')
        (_, parameter), = parameters.items()
        if not parameter.kind == inspect.Parameter.VAR_KEYWORD:
            raise InvalidDecoratorException(f'Must have only one parameter, and that parameter must be a **kwargs '
                                            f'parameter. Instead, found: {parameter}')

    def validate(self, fn: Callable):
        """
        Validates that the function:
        - Is empty (we don't want to be overwriting actual code)
        - is keyword argument only (E.G. has just **kwargs in its argument list)
        :param fn: Function to validate
        :raises: InvalidDecoratorException
        """
        does.ensure_function_empty(fn)
        does.ensure_function_kwarg_only(self.replacing_function)
        does.ensure_output_types_match(fn, self.replacing_function)

    def get_nodes(self, fn: Callable, config) -> Collection[graph.Node]:
        """
        Returns one node which has the replaced functionality
        :param config:
        :param fn:
        :return:
        """
        fn_signature = inspect.signature(fn)
        return [graph.Node(
            fn.__name__,
            doc_string=fn.__doc__ if fn.__doc__ is not None else '',
            callabl=self.replacing_function,
            input_types={key: value.annotation for key, value in fn_signature.parameters.items()},
            typ=fn_signature.return_annotation)]
