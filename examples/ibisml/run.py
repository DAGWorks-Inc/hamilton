from hamilton import driver
from hamilton.execution.executors import SynchronousLocalTaskExecutor
from hamilton.plugins.h_tqdm import ProgressBar


def view_expression(expression, **kwargs):
    """View an Ibis expression

    see graphviz reference for `.render()` kwargs
    ref: https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render
    """
    import ibis.expr.visualize as viz

    dot = viz.to_graph(expression)
    dot.render(**kwargs)
    return dot


def main(model: str):
    import model_training
    import table_dataflow

    config = {"model": model}
    final_vars = ["full_model", "fitted_recipe", "cross_validation_scores"]

    # build the Driver from modules
    dr = (
        driver.Builder()
        .with_modules(table_dataflow, model_training)
        .with_config(config)
        .with_adapters(ProgressBar())
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(SynchronousLocalTaskExecutor())
        .build()
    )

    inputs = dict(
        raw_data_path="../data_quality/simple/Absenteeism_at_work.csv",
        feature_selection=[
            "has_children",
            "has_pet",
            "is_summer_brazil",
            "service_time",
            "seasons",
            "disciplinary_failure",
            "absenteeism_time_in_hours",
        ],
        label="absenteeism_time_in_hours",
    )
    dr.visualize_execution(
        final_vars=final_vars, inputs=inputs, output_file_path="cross_validation.png"
    )

    res = dr.execute(final_vars, inputs=inputs)

    print("Dataflow result keys: ", list(res.keys()))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["linear", "random_forest", "boosting"])
    args = parser.parse_args()

    main(model=args.model)
