import inspect
import sys
import typing
from collections import defaultdict
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Tuple, Type, Union

_sys_version_info = sys.version_info
_version_tuple = (_sys_version_info.major, _sys_version_info.minor, _sys_version_info.micro)

if _version_tuple < (3, 11, 0):
    pass
else:
    pass

import pandas as pd

# Copied this over from function_graph
# TODO -- determine the best place to put this code
from hamilton import node
from hamilton.function_modifiers import base
from hamilton.function_modifiers.expanders import extract_columns
from hamilton.function_modifiers.recursive import assign_namespace, prune_nodes, subdag


class with_columns(base.NodeInjector):
    """Initializes a with_columns decorator for pandas. This allows you to efficiently run groups of map operations on a dataframe.

    Here's an example of calling it -- if you've seen ``@subdag``, you should be familiar with
    the concepts:

    .. code-block:: python

        # my_module.py
        def a(a_from_df: pd.Series) -> pd.Series:
            return _process(a)

        def b(b_from_df: pd.Series) -> pd.Series:
            return _process(b)

        def a_b_average(a_from_df: pd.Series, b_from_df: pd.Series) -> pd.Series:
            return (a_from_df + b_from_df) / 2


    .. code-block:: python

        # with_columns_module.py
        def a_plus_b(a: pd.Series, b: pd.Series) -> pd.Series:
            return a + b


        # the with_columns call
        @with_columns(
            *[my_module], # Load from any module
            *[a_plus_b], # or list operations directly
            columns_to_pass=["a_from_df", "b_from_df"], # The columns to pass from the dataframe to
            # the subdag
            select=["a", "b", "a_plus_b", "a_b_average"], # The columns to select from the dataframe
        )
        def final_df(initial_df: pd.DataFrame) -> pd.DataFrame:
            # process, or just return unprocessed
            ...

    In this instance the ``initial_df`` would get two columns added: ``a_plus_b`` and ``a_b_average``.

    The operations are applied in topological order. This allows you to
    express the operations individually, making it easy to unit-test and reuse.

    Note that the operation is "append", meaning that the columns that are selected are appended
    onto the dataframe.

    If the function takes multiple dataframes, the dataframe input to process will always be
    the first argument. This will be passed to the subdag, transformed, and passed back to the function.
    This follows the hamilton rule of reference by parameter name. To demonstarte this, in the code
    above, the dataframe that is passed to the subdag is `initial_df`. That is transformed
    by the subdag, and then returned as the final dataframe.

    You can read it as:

    "final_df is a function that transforms the upstream dataframe initial_df, running the transformations
    from my_module. It starts with the columns a_from_df and b_from_df, and then adds the columns
    a, b, and a_plus_b to the dataframe. It then returns the dataframe, and does some processing on it."

    In case you need more flexibility you can alternatively use ``pass_dataframe_as``, for example,

    .. code-block:: python

            # with_columns_module.py
            def a_from_df(initial_df: pd.Series) -> pd.Series:
                return initial_df["a_from_df"] / 100

        def b_from_df(initial_df: pd.Series) -> pd.Series:
                return initial_df["b_from_df"] / 100


            # the with_columns call
            @with_columns(
                *[my_module],
                *[a_from_df],
                columns_to_pass=["a_from_df", "b_from_df"],
                select=["a_from_df", "b_from_df", "a", "b", "a_plus_b", "a_b_average"],
            )
            def final_df(initial_df: pd.DataFrame) -> pd.DataFrame:
                # process, or just return unprocessed
                ...

    the above would output a dataframe where the two columns ``a_from_df`` and ``b_from_df`` get
    overwritten.
    """

    @staticmethod
    def _check_for_duplicates(nodes_: List[node.Node]) -> bool:
        """Ensures that we don't run into name clashing of columns and group operations.

        In the case when we extract columns for the user, because ``columns_to_pass`` was used, we want
        to safeguard against nameclashing with functions that are passed into ``with_columns`` - i.e.
        there are no functions that have the same name as the columns. This effectively means that
        using ``columns_to_pass`` will only append new columns to the dataframe and for changing
        existing columns ``pass_dataframe_as`` needs to be used.
        """
        node_counter = defaultdict(int)
        for node_ in nodes_:
            node_counter[node_.name] += 1
            if node_counter[node_.name] > 1:
                return True
        return False

    def __init__(
        self,
        *load_from: Union[Callable, ModuleType],
        columns_to_pass: List[str] = None,
        pass_dataframe_as: str = None,
        select: List[str] = None,
        namespace: str = None,
        config_required: List[str] = None,
    ):
        """Instantiates a ``@with_column`` decorator.

        :param load_from: The functions or modules that will be used to generate the group of map operations.
        :param columns_to_pass: The initial schema of the dataframe. This is used to determine which
            upstream inputs should be taken from the dataframe, and which shouldn't. Note that, if this is
            left empty (and external_inputs is as well), we will assume that all dependencies come
            from the dataframe. This cannot be used in conjunction with pass_dataframe_as.
        :param pass_dataframe_as: The name of the dataframe that we're modifying, as known to the subdag.
            If you pass this in, you are responsible for extracting columns out. If not provided, you have
            to pass columns_to_pass in, and we will extract the columns out for you.
        :param namespace: The namespace of the nodes, so they don't clash with the global namespace
            and so this can be reused. If its left out, there will be no namespace (in which case you'll want
            to be careful about repeating it/reusing the nodes in other parts of the DAG.)
        :param config_required: the list of config keys that are required to resolve any functions. Pass in None\
            if you want the functions/modules to have access to all possible config.
        """

        self.subdag_functions = subdag.collect_functions(load_from)

        if select is None:
            raise ValueError("Please specify at least one column to append or update.")
        else:
            self.select = select

        if (pass_dataframe_as is not None and columns_to_pass is not None) or (
            pass_dataframe_as is None and columns_to_pass is None
        ):
            raise ValueError(
                "You must specify only one of columns_to_pass and "
                "pass_dataframe_as. "
                "This is because specifying pass_dataframe_as injects into "
                "the set of columns, allowing you to perform your own extraction"
                "from the dataframe. We then execute all columns in the sbudag"
                "in order, passing in that initial dataframe. If you want"
                "to reference columns in your code, you'll have to specify "
                "the set of initial columns, and allow the subdag decorator "
                "to inject the dataframe through. The initial columns tell "
                "us which parameters to take from that dataframe, so we can"
                "feed the right data into the right columns."
            )

        self.initial_schema = columns_to_pass
        self.dataframe_subdag_param = pass_dataframe_as
        self.namespace = namespace
        self.config_required = config_required

    def required_config(self) -> List[str]:
        return self.config_required

    def _create_column_nodes(
        self, inject_parameter: str, params: Dict[str, Type[Type]]
    ) -> List[node.Node]:
        output_type = params[inject_parameter]

        def temp_fn(**kwargs) -> pd.DataFrame:
            return kwargs[inject_parameter]

        # We recreate the df node to use extract columns
        temp_node = node.Node(
            name=inject_parameter,
            typ=output_type,
            callabl=temp_fn,
            input_types={inject_parameter: output_type},
        )

        extract_columns_decorator = extract_columns(*self.initial_schema)

        out_nodes = extract_columns_decorator.transform_node(temp_node, config={}, fn=temp_fn)
        return out_nodes[1:]

    def _get_inital_nodes(
        self, fn: Callable, params: Dict[str, Type[Type]]
    ) -> Tuple[str, Collection[node.Node]]:
        """Selects the correct dataframe and optionally extracts out columns."""
        initial_nodes = []
        if self.dataframe_subdag_param is not None:
            inject_parameter = self.dataframe_subdag_param
        else:
            # If we don't have a specified dataframe we assume it's the first argument
            sig = inspect.signature(fn)
            inject_parameter = list(sig.parameters.values())[0].name
            input_types = typing.get_type_hints(fn)

            if not input_types[inject_parameter] == pd.DataFrame:
                raise ValueError(
                    "First argument has to be a pandas DataFrame. If you wish to use a "
                    "different argument, please use `pass_dataframe_as` option."
                )

            initial_nodes.extend(
                self._create_column_nodes(inject_parameter=inject_parameter, params=params)
            )

        if inject_parameter not in params:
            raise base.InvalidDecoratorException(
                f"Function: {fn.__name__} has a first parameter that is not a dependency. "
                f"@with_columns requires the parameter names to match the function parameters. "
                f"Thus it might not be compatible with some other decorators"
            )

        return inject_parameter, initial_nodes

    def _create_merge_node(self, upstream_node: str, node_name: str) -> node.Node:
        "Node that adds to / overrides columns for the original dataframe based on selected output."

        def new_callable(**kwargs) -> Any:
            df = kwargs[upstream_node]
            columns_to_append = {}
            for column in self.select:
                columns_to_append[column] = kwargs[column]

            return df.assign(**columns_to_append)

        input_map = {column: pd.Series for column in self.select}
        input_map[upstream_node] = pd.DataFrame

        return node.Node(
            name=node_name,
            typ=pd.DataFrame,
            callabl=new_callable,
            input_types=input_map,
        )

    def inject_nodes(
        self, params: Dict[str, Type[Type]], config: Dict[str, Any], fn: Callable
    ) -> Tuple[List[node.Node], Dict[str, str]]:
        namespace = fn.__name__ if self.namespace is None else self.namespace

        inject_parameter, initial_nodes = self._get_inital_nodes(fn=fn, params=params)

        subdag_nodes = subdag.collect_nodes(config, self.subdag_functions)

        if with_columns._check_for_duplicates(initial_nodes + subdag_nodes):
            raise ValueError(
                "You can only specify columns once. You used `columns_to_pass` and we "
                "extract the columns for you. In this case they cannot be overwritten -- only new columns get "
                "appended. If you want to modify in-place columns pass in a dataframe and "
                "extract + modify the columns and afterwards select them."
            )

        pruned_nodes = prune_nodes(subdag_nodes, self.select)
        if len(pruned_nodes) == 0:
            raise ValueError(
                f"No nodes found upstream from select columns: {self.select} for function: "
                f"{fn.__qualname__}"
            )

        merge_node = self._create_merge_node(inject_parameter, node_name="__append")

        output_nodes = initial_nodes + pruned_nodes + [merge_node]
        output_nodes = subdag.add_namespace(output_nodes, namespace)
        return output_nodes, {inject_parameter: assign_namespace(merge_node.name, namespace)}

    def validate(self, fn: Callable):
        pass
