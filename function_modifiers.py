import abc
import functools
import inspect
from typing import Dict, Callable, Collection

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

    @abc.abstractmethod
    def get_nodes(self, fn: Callable) -> Collection[graph.Node]:
        """Given a function, converts it to a series of nodes that it produces.

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
        if hasattr(fn, 'nodes'):
            raise Exception(f'Only one expander annotation allowed at a time. Function {fn} already has an expander '
                            f'annotation.')
        fn.nodes = self.get_nodes(fn)
        return fn


class parametrized(NodeExpander):
    def __init__(self, parameter: str, assigned_output: Dict[str, str]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to a function in which the parameter value is replaced by that specific value.

        :param parameter: Parameter to expand on.
        :param assigned_output: A map of values to parameter names.
        """
        self.parameter = parameter
        self.assigned_output = assigned_output

    def get_nodes(self, fn: Callable) -> Collection[graph.Node]:
        """For each parameter value, loop through, partially curry the function, and output a node.

        :param fn: Function to operate on.
        :return: A collection of nodes, each of which is parametrized.
        """
        nodes = []
        input_types = {param_name: param.annotation for param_name, param in inspect.signature(fn).parameters.items()
                       if param_name != self.parameter}
        for value, node_name in self.assigned_output.items():
            nodes.append(
                graph.Node(
                    node_name,
                    inspect.signature(fn).return_annotation,
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
    def __init__(self, *columns: str):
        """Constructor for a modifier that expands a single function into the following nodes:
        - n functions, each of which take in the original dataframe and output a specific column
        - 1 function that outputs the original dataframe

        :param columns: Columns to extract
        """
        self.columns = columns

    def validate(self, fn: Callable):
        """A function is invalid if it does not output a dataframe.

        :param fn: Function to validate.
        :raises: InvalidDecoratorException If the function does not output a Dataframe
        """
        output_type = inspect.signature(fn).return_annotation
        if not issubclass(output_type, pd.DataFrame):
            raise InvalidDecoratorException(
                f'For extracting columns, output type must be pandas dataframe, not: {output_type}')

    def get_nodes(self, fn: Callable) -> Collection[graph.Node]:
        """For each column to extract, output a node that extracts that column. Also, output the original dataframe
        generator.

        :param fn: Function to extract columns from. Must output a dataframe.
        :return: A collection of nodes -- one for the original dataframe generator, and another for each column to extract.
        """
        node_name = fn.__name__
        output_nodes = [graph.Node(node_name, typ=pd.DataFrame, callabl=fn)]
        for column in self.columns:
            def extractor_fn(column_to_extract=column, **kwargs):  # avoiding problems with closures
                return kwargs[node_name][column_to_extract]

            output_nodes.append(
                graph.Node(
                    column,
                    pd.Series,
                    extractor_fn,
                    input_types={node_name: pd.DataFrame}))
        return output_nodes
