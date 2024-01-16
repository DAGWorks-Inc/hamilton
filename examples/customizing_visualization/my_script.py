import logging
import sys
from typing import Optional, Tuple

import pandas as pd

from hamilton import driver, graph_types

logging.basicConfig(stream=sys.stdout)
config = {}

inputs = {  # load from actuals or wherever -- this is our initial data we use as input.
    # Note: these values don't have to be all series, they could be a scalar.
    "signups": pd.Series([1, 10, 50, 100, 200, 400]),
    "spend": pd.Series([10, 10, 20, 40, 40, 50]),
}
import my_functions

dr = driver.Driver(config, my_functions)  # can pass in multiple modules
# we need to specify what we want in the final dataframe. These can be string names, or function references.
output_columns = [
    "spend",
    "signups",
    my_functions.avg_3wk_spend,  # could just pass "avg_3wk_spend" here
    my_functions.spend_per_signup,  # could just pass "spend_per_signup" here
    my_functions.spend_zero_mean_unit_variance,  # could just pass "spend_zero_mean_unit_variance" here
]


def custom_style(
    *, node: graph_types.HamiltonNode, node_class: str
) -> Tuple[dict, Optional[str], Optional[str]]:
    """Custom style function for the visualization.

    This function is called for each node in the graph, and allows you to customize the style of the node.
    The node class relates to base styles that Hamilton applies. E.g. function vs materializer

    :param node: the node that Hamilton is styling.
    :param node_class: the base class of the node that Hamilton will use to style it.
    :return: a triple of (style, node_class, legend_name) where
        style is a dictionary of style attributes,
        node_class is the base class style you want to use - we recommend using what's passed in,
        legend_name is what to put in the legend for the provided style. Return None if you don't want to add a legend entry.
    """
    if node.tags.get("some_key") == "some_value":
        return {"fillcolor": "blue"}, node_class, "some_key"
    elif node.tags.get("module") == "my_functions":
        return {"fillcolor": "orange"}, node_class, "features"
    return {}, None, None


print(dr.execute(output_columns, inputs=inputs))
dr.visualize_execution(
    output_columns,
    inputs=inputs,
    output_file_path="./viz_execution.dot",
    render_kwargs={"format": "png"},
)
dr.visualize_execution(
    output_columns,
    inputs=inputs,
    output_file_path="./viz_execution_custom.dot",
    render_kwargs={"format": "png"},
    custom_style_function=custom_style,
)
dr.visualize_path_between(
    "spend_mean",
    "spend_zero_mean_unit_variance",
    "./viz_path_between.dot",
    {"format": "png"},
    strict_path_visualization=False,
)
dr.visualize_path_between(
    "spend_mean",
    "spend_zero_mean_unit_variance",
    "./viz_path_between_custom.dot",
    {"format": "png"},
    strict_path_visualization=False,
    custom_style_function=custom_style,
)
dr.visualize_materialization(
    additional_vars=output_columns,
    inputs=inputs,
    output_file_path="./viz_materialization.dot",
    render_kwargs={"format": "png"},
)
dr.visualize_materialization(
    additional_vars=output_columns,
    inputs=inputs,
    output_file_path="./viz_materialization_custom.dot",
    render_kwargs={"format": "png"},
    custom_style_function=custom_style,
)

dr.display_all_functions("./viz_all_functions.dot", {"format": "png"})
dr.display_all_functions(
    "./viz_all_functions_custom.dot", {"format": "png"}, custom_style_function=custom_style
)
