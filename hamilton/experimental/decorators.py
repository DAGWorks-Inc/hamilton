import dataclasses
import functools
import uuid
from typing import Any, Callable, Collection, Dict, List, Tuple

import pandas as pd

from hamilton import function_modifiers, function_modifiers_base, node
from hamilton.function_modifiers import (
    InvalidDecoratorException,
    ParametrizedDependency,
    source,
    value,
)


def get_dep_type(dep_type: str):
    # cheap code to do mapping, we can clean this up if this is the API we choose
    if dep_type == "value" or dep_type == "out":
        return value
    elif dep_type == "source":
        return source
    raise ValueError(f"Invalid dep type: {dep_type}")


def get_index_levels(index: pd.MultiIndex) -> List[list]:
    out = [[] for _ in index[0]]
    for specific_index in index:
        for i, key in enumerate(specific_index):
            out[i].append(key)
    return out


@dataclasses.dataclass
class ParameterizedExtract:
    outputs: Tuple[str, ...]
    input_mapping: Dict[str, ParametrizedDependency]


class parameterize_extract(function_modifiers_base.NodeExpander):
    def __init__(self, *extract_config: ParameterizedExtract, reassign_columns: bool = True):
        """Initializes a `parameterized_extract` decorator. Note this currently works for series,
        but the plan is to extend it to fields as well...

        :param extract_config: A configuration consisting of a list ParameterizedExtract classes
        These contain the information of a `@parameterized` and `@extract...` together.
        :param reassign_columns: Whether we want to reassign the columns as part of the function
        """
        self.extract_config = extract_config
        self.reassign_columns = reassign_columns

    def expand_node(
        self, node_: node.Node, config: Dict[str, Any], fn: Callable
    ) -> Collection[node.Node]:
        output_nodes = []
        for parameterization in self.extract_config:

            @functools.wraps(fn)
            def wrapper_fn(*args, _output_columns=parameterization.outputs, **kwargs):
                df_out = fn(*args, **kwargs)
                df_out.columns = _output_columns
                return df_out

            # This is a bit of a hack so we can rewrite the function to have the right columns
            # (as extract_columns doesn't allow for pos-based arguments)
            # In reality, this should be a node generator as well, but
            new_node = node_.copy_with(callabl=wrapper_fn)
            fn_to_call = wrapper_fn if self.reassign_columns else fn
            temp_name = "temp_" + str(uuid.uuid4()).replace("-", "_")  # oof cheap hack
            parameterization_decorator = function_modifiers.parameterize(
                **{temp_name: parameterization.input_mapping}
            )
            (parameterized_node,) = parameterization_decorator.expand_node(
                new_node, config, fn_to_call
            )
            extract_columns_decorator = function_modifiers.extract_columns(
                *parameterization.outputs
            )
            output_nodes.extend(
                extract_columns_decorator.expand_node(
                    parameterized_node, config, parameterized_node.callable
                )
            )

        return output_nodes

    def validate(self, fn: Callable):
        pass


class parameterize_frame(parameterize_extract):
    def __init__(self, parameterization: pd.DataFrame):
        """Instiantiates a parameterize_frame decorator. This uses a
        dataframe to specify a set of extracts + parameterizations.

        :param parameterization: Parameterization dataframe. This is of a specific shape:
        1. Index
        - Level 0: list of parameter names
        - Level 1: types of things to inject, either
            - "out" (meaning this is an output),
            - "value" (meaning this is a literal value)
            - "source" (meaning this node comes from an upstream value)
        2. Contents
        - Each row corresponds to the index. Each of these corresponds to an output node from this.


        Note your function has to take in the column-names and output a dataframe with those names --
        we will likely change it so that's not the case, and it can just use the position of the columns.
        """
        super(parameterize_frame, self).__init__(
            *parameterize_frame._convert_params(parameterization), reassign_columns=False
        )

    @staticmethod
    def _validate_parameterization(parameterization: pd.DataFrame):
        # TODO -- validate that its a multi-index
        columns = get_index_levels(parameterization.columns)
        if (not len(columns) == 2) or "out" not in columns[1]:
            raise InvalidDecoratorException(
                "Decorator must have a double-index -- first index should be a "
                "list of {output, source, value} strs. Second must be a list of "
                "arguments in your function."
            )

    @staticmethod
    def _convert_params(df: pd.DataFrame) -> List[ParameterizedExtract]:
        parameterize_frame._validate_parameterization(df)
        args, dep_types = get_index_levels(df.columns)
        dep_types_converted = [get_dep_type(val) for val in dep_types]
        out = []
        for _, column_set in df.iterrows():
            parameterization = {
                arg: dep_type(col_value)
                for arg, col_value, dep_type in zip(args, column_set, dep_types_converted)
            }
            extracted_columns = [
                col for col, dep_type in zip(column_set, dep_types) if dep_type == "out"
            ]
            out.append(ParameterizedExtract(extracted_columns, parameterization))
        return out

    def validate(self, fn: Callable):
        pass
        # TODO -- validate against column names


# Examples below
if __name__ == "__main__":
    df = pd.DataFrame(
        [
            ["outseries1a", "outseries2a", "inseries1a", "inseries2a", 5.0],
            ["outseries1b", "outseries2b", "inseries1b", "inseries2b", 0.2],
            # ...
        ],
        # Have to switch as indices have to be unique
        columns=[
            [
                "output1",
                "output2",
                "input1",
                "input2",
                "input3",
            ],  # configure whether column is source or value and also whether it's input ("source", "value") or output ("out")
            ["out", "out", "source", "source", "value"],
        ],
    )  # specify column names (corresponding to function arguments and (if outputting multiple columns) output dataframe columns)

    @parameterize_frame(df)
    def my_func(
        output1: str, output2: str, input1: pd.Series, input2: pd.Series, input3: float
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {output1: input1 * input2 * input3, output2: input1 + input2 + input3}
        )  # if there's a single column it could maybe just return a series instead and pick up the name from the first column of the dataframe

    @parameterize_extract(
        ParameterizedExtract(
            ("outseries1a", "outseries2a"),
            {"input1": source("inseries1a"), "input2": source("inseries2a"), "input3": value(5.0)},
        ),
        ParameterizedExtract(
            ("outseries1b", "outseries2b"),
            {"input1": source("inseries1b"), "input2": source("inseries2b"), "input3": value(0.2)},
        ),
    )
    def my_func_parameterized_extract(
        input1: pd.Series, input2: pd.Series, input3: float
    ) -> pd.DataFrame:
        print("running my_func_parameterized_extract")
        return pd.concat([input1 * input2 * input3, input1 + input2 + input3], axis=1)

    setattr(my_func_parameterized_extract, "decorated", "false")

    # Test by running the @parameterized_extract decorator
    from hamilton.ad_hoc_utils import create_temporary_module
    from hamilton.driver import Driver

    dr = Driver({}, create_temporary_module(my_func_parameterized_extract))
    dr.visualize_execution(
        final_vars=["outseries1a", "outseries1b", "outseries2a", "outseries2b"],
        output_file_path="./out1.pdf",
        render_kwargs={},
        inputs={
            "inseries1a": pd.Series([1, 2]),
            "inseries1b": pd.Series([2, 3]),
            "inseries2a": pd.Series([3, 4]),
            "inseries2b": pd.Series([4, 5]),
        },
    )

    df_1 = dr.execute(
        final_vars=["outseries1a", "outseries1b", "outseries2a", "outseries2b"],
        # final_vars=["outseries1a", "outseries2a"],
        inputs={
            "inseries1a": pd.Series([1, 2]),
            "inseries1b": pd.Series([2, 3]),
            "inseries2a": pd.Series([3, 4]),
            "inseries2b": pd.Series([4, 5]),
        },
    )
    print(df_1)

    # Test by running the @parameterized_extract decorator
    dr = Driver({}, create_temporary_module(my_func))
    dr.visualize_execution(
        final_vars=["outseries1a", "outseries1b", "outseries2a", "outseries2b"],
        output_file_path="./out2.pdf",
        render_kwargs={},
        inputs={
            "inseries1a": pd.Series([1, 2]),
            "inseries1b": pd.Series([2, 3]),
            "inseries2a": pd.Series([3, 4]),
            "inseries2b": pd.Series([4, 5]),
        },
    )

    df_2 = dr.execute(
        final_vars=["outseries1a", "outseries1b", "outseries2a", "outseries2b"],
        # final_vars=["outseries1a", "outseries2a"],
        inputs={
            "inseries1a": pd.Series([1, 2]),
            "inseries1b": pd.Series([2, 3]),
            "inseries2a": pd.Series([3, 4]),
            "inseries2b": pd.Series([4, 5]),
        },
    )
    print(df_2)
