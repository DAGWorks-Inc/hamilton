# Customizing the visualization of your graph

We enable you to pass in a custom styling function to Hamilton, so that you can customize the style of the nodes in the graph.

This is useful if you want to highlight certain nodes, or if you want to change the style of the nodes based on their tags.

An example function is shown below:

```python
from hamilton import graph_types
from typing import Tuple, Optional

def custom_style(*, node: graph_types.HamiltonNode, node_class: str) -> Tuple[dict, Optional[str], Optional[str]]:
    """Custom style function for the visualization.

    This function is called for each node in the graph, and allows you to customize the style of the node.
    The node class relates to base styles that Hamilton applies. E.g. function vs materializer.

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

```
Note:
1. The requirement that the signature of the function be kwargs only, taking in node and node_class.
2. The return needs to be a triple of (style, node_class, legend_name) where:

    * style is a dictionary of style attributes,
    * node_class is the base class style you want to use - we recommend using what's passed in,
    * legend_name is what to put in the legend for the provided style. Pass back None if you don't want to add a legend entry.

3. The style values need to be valid graphviz node style attributes. See [here](https://graphviz.org/doc/info/attrs.html) for more details.
4. For the execution focused visualizations, your custom styles will be applied before the modifiers for outputs and overrides is applied.

If you need more customization, we suggest getting the graphviz object back, and then modifying it yourself.
