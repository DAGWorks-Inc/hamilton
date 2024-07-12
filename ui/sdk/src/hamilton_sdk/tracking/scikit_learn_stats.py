from typing import List

from hamilton_sdk.tracking import data_observation
from sklearn.base import BaseEstimator

"""Module that houses functions to compute statistics on numpy objects"""


@data_observation.compute_stats.register(BaseEstimator)
def get_estimator_params(result, *args, **kwargs) -> data_observation.ObservationType:
    """
    ref: https://scikit-learn.org/stable/auto_examples/miscellaneous/plot_display_object_visualization.html
    """
    return {
        "name": "Parameters",
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(result)),
            "value": result.get_params(deep=True),
        },
        "observability_schema_version": "0.0.2",
    }


@data_observation.compute_additional_results.register(BaseEstimator)
def get_estimator_html(result, *args, **kwargs) -> List[data_observation.ObservationType]:
    return [
        {
            "name": "Components",
            "observability_type": "html",
            "observability_value": {"html": result._repr_html_inner()},  # get_params(deep=True),
            "observability_schema_version": "0.0.1",
        }
    ]
