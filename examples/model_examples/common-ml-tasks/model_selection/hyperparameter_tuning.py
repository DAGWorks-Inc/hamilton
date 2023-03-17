from typing import List, Tuple

"""This file shows a basic approach to hyperparameter tuning with the "run n iterations,
select the best" strategy. This does the following:
1. Set up an initial iteration
2. execute n_iterations (passed from config) number of iterations of the optimization routine
3. Select the best iteration

This makes use of the @parameterized_subdag/resolve pattern to run one subdag for each iteration,
consisting of the optimization routine.

Note that you have a lot of options as to how to set up optimization, and this presumes a stateless
one. See instructions in the iteration_0/iteration_n subdag for more details.
"""


import optimizer_iteration
import pandas as pd

from hamilton.function_modifiers import (
    ResolveAt,
    inject,
    parameterized_subdag,
    resolve,
    source,
    subdag,
    value,
)
from hamilton.function_modifiers.dependencies import group


def training_dataset() -> pd.DataFrame:
    """Mock function to return training data.

    :return: Whatever you want.
    """
    pass


def evaluation_dataset() -> pd.DataFrame:
    """Mock function to return evaluation data.

    :return: Whatever you want.
    """
    pass


Iteration = Tuple[optimizer_iteration.Model, optimizer_iteration.EvaluationMetrics]


@subdag(
    optimizer_iteration,
    inputs={
        "prior_hyperparameters": source("initial_hyperparameters"),
        "prior_evaluation_metrics": value(None),
    },
)
def iteration_0(
    evaluation_metrics: optimizer_iteration.EvaluationMetrics,
    trained_model: optimizer_iteration.Model,
) -> Iteration:
    """First iteration of the hyperparameter routine. This could easily be combined with the
    subdag below, but it is separated to avoid ugly parameterized if statements (E.G. the source
    of `hyperparamters` versus `prior_hyperparameters`). Another option would be to have
    `iteration_0_hyperparameters` and `iteration_0_evaluation_metrics` as inputs to the overall
    DAG -- up to you!

    :param evaluation_metrics: Metrics to use to evaluate the model
    :param trained_model: Model that was trained
    :return: Tuple of the hyperparameters and evaluation metrics.
    This is purely for easy access later, so that if one queries for `iteration_i`
    they get both hyperparameters and evaluation metrics.
    """
    return (trained_model, evaluation_metrics)


@resolve(
    when=ResolveAt.CONFIG_AVAILABLE,
    decorate_with=lambda n_iterations: parameterized_subdag(
        optimizer_iteration,
        **{
            f"iteration_{i}": {
                "inputs": {
                    "prior_hyperparameters": source(f"iteration_{i - 1}.hyperparameters"),
                    "prior_evaluation_metrics": source(f"iteration_{i - 1}.evaluation_metrics"),
                }
            }
            for i in range(1, n_iterations)
        },
    ),
)
def iteration_n(
    evaluation_metrics: optimizer_iteration.EvaluationMetrics,
    trained_model: optimizer_iteration.Model,
) -> Iteration:
    """Parameterized subdag to run the hyperparameter optimization routine.
    See description above for more clarity.

    :param evaluation_metrics: Metrics to evaluate the
    :param trained_model: Model that was trained.
    :return: Tuple of the hyperparameters and evaluation metrics.
    """
    return (trained_model, evaluation_metrics)


@resolve(
    when=ResolveAt.CONFIG_AVAILABLE,
    decorate_with=lambda n_iterations: inject(
        iterations=group(*[source(f"iteration_{i}") for i in range(n_iterations)])
    ),
)
def best_iteration(iterations: List[Iteration]) -> Iteration:
    """Returns the best iteration of all of them.

    TODO -- implement me!

    :param iterations:
    :return:
    """
    pass
