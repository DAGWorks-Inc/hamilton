import hyperparameter_tuning
import optimizer_iteration

from hamilton.function_modifiers import source, subdag

"""This file contains a pipeline of pipelines.
What this does is:
1. Call out to a subdag for each of the model optimization routines
2. Pass specific parameters for them
3. Combines them to form a "comparator" to do something (vote/select, or combine) the models.

Note that this is slightly opinionated about the workflow. Specifically,
This approach requires that you hardcode the types of models in code.
This makes sense if they're each radically different (E.G. decision trees versus
logic regression versus calling out to GPT-4 and seeing what happens),
but if they're variants on the same (effectively different groups of hyperparameters
that a gaussian process might not capture well), then you might want to make this more configuration
driven. To do this, you can use @parameterize_subdag/resolve.
"""


@subdag(
    hyperparameter_tuning,
    inputs={"initial_hyperparameters": source("initial_hyperparameters_model_a")},
    config={"n_iterations": 25, "model_type": "model_a"},
)
def model_a(best_iteration: hyperparameter_tuning.Iteration) -> hyperparameter_tuning.Iteration:
    """Training routine (subdag) for model A.
    This is just a pass-through, although it could do whatever you want.

    :param best_iteration:
    :return:
    """
    return best_iteration


@subdag(
    hyperparameter_tuning,
    inputs={"initial_hyperparameters": source("initial_hyperparameters_model_b")},
    config={"n_iterations": 14, "model_type": "model_b"},
)
def model_b(best_iteration: hyperparameter_tuning.Iteration) -> hyperparameter_tuning.Iteration:
    """Training routine (subdag) for model B.
    This is just a pass-through, although it could do whatever you want.

    :param best_iteration:
    :return:
    """
    return best_iteration


@subdag(
    hyperparameter_tuning,
    inputs={"initial_hyperparameters": source("initial_hyperparameters_model_b")},
    config={"n_iterations": 19, "model_type": "model_c"},
)
def model_c(best_iteration: hyperparameter_tuning.Iteration) -> hyperparameter_tuning.Iteration:
    """Training routine (subdag) for model B.
    This is just a pass-through, although it could do whatever you want.

    :param best_iteration:
    :return:
    """
    return best_iteration


def best_model(
    model_a: hyperparameter_tuning.Iteration,
    model_b: hyperparameter_tuning.Iteration,
    model_c: hyperparameter_tuning.Iteration,
) -> optimizer_iteration.Model:
    """Some comparator/selector to chose which model. Note this could do anything you want
    (even combine a model), but for now it'll do nothing (you can fill this in!)

    Should you want to retrain after this, you could pull in the training/holdout
    set and execute it in another function.

    :param model_a: Model A trained + evaluation metrics
    :param model_b: Model B trained + evaluation metrics
    :param model_c: Model C trained + evaluation metrics
    :return: The model we want to use.
    """
