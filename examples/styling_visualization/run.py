import data_loading
import features
import model_pipeline
import sets

from hamilton import driver


def custom_style(*, node, node_class):
    """Custom style function for the visualization."""
    if node.tags.get("PII"):
        style = ({"fillcolor": "aquamarine"}, node_class, "PII")

    elif node.tags.get("owner") == "data-science":
        style = ({"fillcolor": "olive"}, node_class, "data-science")

    elif node.tags.get("owner") == "data-engineering":
        style = ({"fillcolor": "lightsalmon"}, node_class, "data-engineering")

    else:
        style = ({}, node_class, None)

    return style


dr = driver.Builder().with_modules(data_loading, features, sets, model_pipeline).build()


dr.display_all_functions("dag", custom_style_function=custom_style)
