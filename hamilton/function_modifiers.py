import functools
import inspect
from typing import Dict, Callable, Collection, Tuple, Union, Any, Type

import pandas as pd

from hamilton import node
from hamilton.function_modifiers_base import NodeCreator, NodeResolver, NodeExpander, NodeTransformer
from hamilton.models import BaseModel

"""
Annotations for modifying the way functions get added to the DAG.
All user-facing annotation classes are lowercase as they're meant to be used
as annotations. They are classes to hold state and subclass common functionality.
"""


class InvalidDecoratorException(Exception):
    pass


class parametrized(NodeExpander):
    def __init__(self, parameter: str, assigned_output: Dict[Tuple[str, str], Any]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to a function in which the parameter value is replaced by that *specific value*.

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

    def validate(self, fn: Callable):
        """A function is invalid if it does not have the requested parameter.

        :param fn: Function to validate against this annotation.
        :raises: InvalidDecoratorException If the function does not have the requested parameter
        """
        signature = inspect.signature(fn)
        if self.parameter not in signature.parameters.keys():
            raise InvalidDecoratorException(
                f'Annotation is invalid -- no such parameter {self.parameter} in function {fn}')

    def expand_node(self, node_: node.Node, config: Dict[str, Any]) -> Collection[node.Node]:
        """For each parameter value, loop through, partially curry the function, and output a node.

                :param config:
                :param fn: Function to operate on.
                :return: A collection of nodes, each of which is parametrized.
                """
        input_types = node_.input_types
        nodes = []
        for (node_name, node_doc), value in self.assigned_output.items():
            nodes.append(
                node.Node(
                    node_name,
                    node_.type,
                    node_doc,
                    functools.partial(node_.callable, **{self.parameter: value}),
                    input_types={key: value for key, (value, _) in input_types.items() if key != self.parameter}))
        return nodes


class parametrized_input(NodeExpander):

    def __init__(self, parameter: str, variable_inputs: Dict[str, Tuple[str, str]]):
        """Constructor for a modifier that expands a single function into n, each of which
        corresponds to the specified parameter replaced by a *specific input column*.

        Note this decorator and `@parametrized` are quite similar, except that the input here is another DAG node,
        i.e. column, rather than some specific value.

        The `parameterized_inputs` allows you keep your code DRY by reusing the same function but replace the inputs
        to create multiple corresponding distinct outputs. The _parameter_ key word argument has to match one of the
        arguments in the function. The rest of the arguments are pulled from items inside the DAG.
        The _assigned_inputs_ key word argument takes in a
        dictionary of input_column -> tuple(Output Name, Documentation string).

        :param parameter: Parameter to expand on.
        :param variable_inputs: A map of tuple of [parameter names, documentation] to values
        """
        self.parameter = parameter
        self.assigned_output = variable_inputs
        for value in variable_inputs.values():
            if not isinstance(value, Tuple):
                raise InvalidDecoratorException(
                    f'assigned_output key is incorrect: {node}. The parameterized decorator needs a dict of '
                    'input column -> [name, description] to function.')

    def expand_node(self, node_: node.Node, config: Dict[str, Any]) -> Collection[node.Node]:
        nodes = []
        fn = node_.callable
        input_types = node_.input_types
        for input_column, (node_name, node_description) in self.assigned_output.items():
            specific_inputs = {key: value for key, (value, _) in input_types.items()}
            specific_inputs[input_column] = specific_inputs.pop(self.parameter)  # replace the name with the new function name so we get the right dependencies

            def new_fn(*args, input_column=input_column, **kwargs):
                kwargs = kwargs.copy()
                kwargs[self.parameter] = kwargs.pop(input_column)
                return fn(*args, **kwargs)

            nodes.append(
                node.Node(
                    node_name,
                    node_.type,
                    node_description,
                    new_fn,
                    input_types=specific_inputs))
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

    def expand_node(self, node_: node.Node, config: Dict[str, Any]) -> Collection[node.Node]:
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

        output_nodes = [node.Node(node_.name, typ=pd.DataFrame, doc_string=base_doc, callabl=df_generator)]

        for column in self.columns:
            doc_string = base_doc  # default doc string of base function.
            if isinstance(column, Tuple):  # Expand tuple into constituents
                column, doc_string = column

            def extractor_fn(column_to_extract: str = column, **kwargs) -> pd.Series:  # avoiding problems with closures
                df = kwargs[node_.name]
                if column_to_extract not in df:
                    raise InvalidDecoratorException(f'No such column: {column_to_extract} produced by {node_.name}. '
                                                    f'It only produced {str(df.columns)}')
                return kwargs[node_.name][column_to_extract]

            output_nodes.append(
                node.Node(column, pd.Series, doc_string, extractor_fn, input_types={node_.name: pd.DataFrame}))
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
    if fn.__code__.co_code not in {_empty_function.__code__.co_code,
                                   _empty_function_with_docstring.__code__.co_code}:
        raise InvalidDecoratorException(f'Function: {fn.__name__} is not empty. Must have only one line that '
                                        f'consists of "pass"')


class does(NodeCreator):
    def __init__(self, replacing_function: Callable):
        """
        Constructor for a modifier that replaces the annotated functions functionality with something else.
        Right now this has a very strict validation requirements to make compliance with the framework easy.
        """
        self.replacing_function = replacing_function

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
        ensure_function_empty(fn)
        does.ensure_function_kwarg_only(self.replacing_function)
        does.ensure_output_types_match(fn, self.replacing_function)

    def generate_node(self, fn: Callable, config) -> node.Node:
        """
        Returns one node which has the replaced functionality
        :param config:
        :param fn:
        :return:
        """
        fn_signature = inspect.signature(fn)
        return node.Node(
            fn.__name__,
            typ=fn_signature.return_annotation,
            doc_string=fn.__doc__ if fn.__doc__ is not None else '',
            callabl=self.replacing_function,
            input_types={key: value.annotation for key, value in fn_signature.parameters.items()})


class model(NodeCreator):
    def __init__(self, model_cls: Type[BaseModel], config_param: str, **extra_model_params):
        """Constructs a model. Takes in a model_cls, whose only construction parameter is a dictionary."""
        self.model_cls = model_cls
        self.config_param = config_param
        self.extra_model_params = extra_model_params

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
            raise InvalidDecoratorException('Models must declare their return type as a pandas Series')
        if len(signature.parameters) > 0:
            raise InvalidDecoratorException('Models must have no parameters -- all are passed in through the config')

    def generate_node(self, fn: Callable, config: Dict[str, Any] = None) -> node.Node:
        if self.config_param not in config:
            raise InvalidDecoratorException(f'Configuration has no parameter: {self.config_param}. Did you define it? If so did you spell it right?')
        fn_name = fn.__name__
        model = self.model_cls(config[self.config_param], fn_name, **self.extra_model_params)
        return node.Node(
            name=fn_name,
            typ=inspect.signature(fn).return_annotation,
            doc_string=fn.__doc__,
            callabl=model.predict,
            input_types={dep: pd.Series for dep in model.get_dependents()})


class config(NodeResolver):
    """Decorator class that resolves a node's function based on  some configuration variable
    Currently, functions that exist in all configurations have to be disjoint.
    E.G. for every config.when(), you can have a config.when_not() that filters the opposite.
    That said, you can have functions that *only* exist in certain configurations without worrying about it.
    """

    def __init__(self, resolves: Callable[[Dict[str, Any]], bool], target_name: str = None):
        self.does_resolve = resolves
        self.target_name = target_name

    @staticmethod
    def _filter_underscores(name: str):
        last_dunder_index = name.rfind('__')
        return name[:last_dunder_index] if last_dunder_index != -1 else name

    def _get_function_name(self, fn: Callable) -> str:
        if self.target_name is not None:
            return self.target_name
        return config._filter_underscores(fn.__name__)

    def resolve(self, fn, configuration: Dict[str, Any]) -> Callable:
        if not self.does_resolve(configuration):
            return None
        fn.__name__ = self._get_function_name(fn)  # TODO -- copy function to not mutate it
        return fn

    def validate(self, fn):
        if fn.__name__.endswith('__'):
            raise InvalidDecoratorException('Config will always use the portion of the function name before the last __. For example, signups__v2 will map to signups, whereas')

    @staticmethod
    def when(name=None, **key_value_pairs) -> 'config':
        """Yields a decorator that resolves the function if all keys in the config are equal to the corresponding value

        :param key_value_pairs: Keys and corresponding values to look up in the config
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(value == configuration.get(key) for key, value in key_value_pairs.items())

        return config(resolves, target_name=name)

    @staticmethod
    def when_not(name=None, **key_value_pairs: Any) -> 'config':
        """Yields a decorator that resolves the function if none keys in the config are equal to the corresponding value

        :param key_value_pairs: Keys and corresponding values to look up in the config
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(value != configuration.get(key) for key, value in key_value_pairs.items())

        return config(resolves, target_name=name)

    @staticmethod
    def when_in(name=None, **key_value_group_pairs: Collection[Any]) -> 'config':
        """Yields a decorator that resolves the function if all of the keys are equal to one of items in the list of values.

        :param key_value_group_pairs: pairs of key-value mappings where the value is a lsit of possible values
        :return: a configuration decorator
        """

        def resolves(configuration: Dict[str, Any]) -> bool:
            return all(configuration.get(key) in value for key, value in key_value_group_pairs.items())

        return config(resolves, target_name=name)

    @staticmethod
    def when_not_in(**key_value_group_pairs: Collection[Any]) -> 'config':
        """Yields a decorator that resolves the function only if none of the keys are in the list of values.

        :param key_value_group_pairs: pairs of key-value mappings where the value is a lsit of possible values
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
            return all(configuration.get(key) not in value for key, value in key_value_group_pairs.items())

        return config(resolves)
