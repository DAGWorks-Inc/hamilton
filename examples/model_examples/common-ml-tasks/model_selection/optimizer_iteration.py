import dataclasses
from random import random
from typing import Any

import pandas as pd

from hamilton.function_modifiers import config, does


def _sleep_random(**kwargs: Any):
    import time

    time.sleep(random() * 0.2)


class Model:
    """Sample class to represent a model."""

    pass


@dataclasses.dataclass
class EvaluationMetrics:
    """Sample dataclass to represent a set of evaluation metrics."""

    score: float


@dataclasses.dataclass
class Hyperparameters:
    """Sample dataclass to represent a set of hyperparameters."""

    some_value: float
    some_other_value: float


@does(_sleep_random)
def hyperparameters(
    prior_hyperparameters: Hyperparameters, prior_evaluation_metrics: EvaluationMetrics = None
) -> Hyperparameters:
    """Yields hyperparameters given a prior set of
    hyperparameters and an evaluation metrics. Note that this assumeds a *stateless* optimizer,
    but this is not the case (unless you're using `random`). Choices are:
    1. Have this take in *all* prior hyperparameters and evaluation metrics, and then
    execute an optimization routine from scratch every time. That's potentially inefficient,
    but it works with a purely functional perspective. You'd basically have to call the optimizer
    in a for-loop.
    2. Have this pass its state through to the next iterations (E.G. return a tuple of hyperparameters,
    state, and have the subdag extract the state and pass it along). Initial state is None. This is
    another clean, functional way to do it, but might require changing the way the upstream optimizer
    works/adding more code around it.
    3. Have an upstream function that returns a stateful object that hamilton does *not* know about.
    This is the easiest to implement, but potentially hacky. Specifically, it could break if you need parallel
    execution and aren't careful -- the stateful object would effectively have to call out to a service.

    Honestly, I recommend (3) for this to test with -- its easy to get started locally, then if you
    truly need a distributed optimization system like sigopt, you could just have it be a resource
    that calls out to external services.  This could also help: https://github.com/DAGWorks-Inc/hamilton/issues/90.

    Curently this just sleeps for a small random period of time to make the
    APM charts look pretty :)

    :param prior_hyperparameters: Hyperparameters from the previous step.
    :param prior_evaluation_metrics: Evaluation metrics from the previous step.
    :return: New hyperparameters to try.
    """
    pass


@config.when(model_type=None)
@does(_sleep_random)
def trained_model__default(
    hyperparameters: Hyperparameters, training_dataset: pd.DataFrame
) -> Model:
    """Trains the model, given the set of hyperparameters and the training data.
    This is the default implementation, when no model type is passed in.

    Curently this just sleeps for a small random period of time to make the
    APM charts look pretty :)

    :param hyperparameters: Hyperparameters to train.
    :return: The trained model object.
    """
    pass


@config.when(model_type="model_a")
@does(_sleep_random)
def trained_model__model_a(
    hyperparameters: Hyperparameters, training_dataset: pd.DataFrame
) -> Model:
    pass
    # return trained_model__default()


@config.when(model_type="model_b")
@does(_sleep_random)
def trained_model__model_b(
    hyperparameters: Hyperparameters, training_dataset: pd.DataFrame
) -> Model:
    """Model training for model b"""
    pass


@config.when(model_type="model_c")
@does(_sleep_random)
def trained_model__model_c(
    hyperparameters: Hyperparameters, training_dataset: pd.DataFrame
) -> Model:
    """Model training for model C"""
    pass


@does(_sleep_random)
def evaluation_metrics(trained_model: Model, evaluation_dataset: pd.DataFrame) -> EvaluationMetrics:
    """Evaluates the model, given the trained model and the evaluation dataset.

    :param trained_model: Model to train
    :param evaluation_dataset: Dataset to evaluate on.
    :return: Evaluation metrics.
    """
    pass
