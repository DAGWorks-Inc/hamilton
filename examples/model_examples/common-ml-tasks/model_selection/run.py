import click
import multi_model_selection
import optimizer_iteration

from hamilton import base
from hamilton.settings import ENABLE_POWER_USER_MODE


@click.group()
def main():
    pass


@main.command()
@click.option("--username", required=False, type=str)
@click.option("--api-key", required=False, type=str)
@click.option("--num-iterations", required=False, type=int, default=20)
def hyperparameter_optimization(username, api_key, num_iterations):
    import hyperparameter_tuning

    if username is None and api_key is None:
        from hamilton import driver

        dr = driver.Driver(
            {"n_iterations": num_iterations, ENABLE_POWER_USER_MODE: True},
            hyperparameter_tuning,
            adapter=base.SimplePythonGraphAdapter(base.DictResult()),
        )
    else:
        from dagworks import driver

        dr = driver.Driver(
            {"n_iterations": num_iterations, ENABLE_POWER_USER_MODE: True},
            hyperparameter_tuning,
            project="ML Hyperparameter Tuning",
            username=username,
            api_key=api_key,
            adapter=base.SimplePythonGraphAdapter(base.DictResult()),
        )
    dr.execute(
        ["best_iteration"],
        inputs={
            "initial_hyperparameters": optimizer_iteration.Hyperparameters(
                some_value=0, some_other_value=0
            )
        },
    )


@main.command()
@click.option("--username", required=False, type=str)
@click.option("--api-key", required=False, type=str)
def model_selection(username, api_key):
    if username is None and api_key is None:
        from hamilton import driver

        dr = driver.Driver(
            {ENABLE_POWER_USER_MODE: True},
            multi_model_selection,
            adapter=base.SimplePythonGraphAdapter(base.DictResult()),
        )
    else:
        from dagworks import driver

        dr = driver.Driver(
            {ENABLE_POWER_USER_MODE: True},
            multi_model_selection,
            project="ML Model Selection + hyperparameter Tuning",
            username=username,
            api_key=api_key,
            adapter=base.SimplePythonGraphAdapter(base.DictResult()),
        )
    dr.execute(
        ["best_model"],
        inputs={
            "initial_hyperparameters_model_a": optimizer_iteration.Hyperparameters(
                some_value=0, some_other_value=0
            ),
            "initial_hyperparameters_model_b": optimizer_iteration.Hyperparameters(
                some_value=0, some_other_value=0
            ),
        },
    )


if __name__ == "__main__":
    main()
