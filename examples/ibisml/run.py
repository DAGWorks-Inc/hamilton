from hamilton import driver
from hamilton.plugins.h_tqdm import ProgressBar
from hamilton.execution.executors import SynchronousLocalTaskExecutor


def view_expression(expression, **kwargs):
    """View an Ibis expression

    see graphviz reference for `.render()` kwargs
    ref: https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render
    """
    import ibis.expr.visualize as viz

    dot = viz.to_graph(expression)
    dot.render(**kwargs)
    return dot


def main(level: str, model: str):
    dataflow_components = []
    config = {}
    final_vars = ["feature_set"]

    if level == "column":
        import column_dataflow

        dataflow_components.append(column_dataflow)
    elif level == "table":
        import table_dataflow

        dataflow_components.append(table_dataflow)
    else:
        raise ValueError("`level` must be in ['column', 'table']")

    if model:
        import model_training

        dataflow_components.append(model_training)
        config["model"] = model
        final_vars.extend(["full_model", "fitted_recipe", "cross_validation_scores"])

    # build the Driver from modules
    dr = (
        driver.Builder()
        .with_modules(*dataflow_components)
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
    view_expression(res["feature_set"], filename="ibis_feature_set", format="png")

    print("Dataflow result keys: ", list(res.keys()))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--level", choices=["column", "table"])
    parser.add_argument("--model", choices=["linear", "random_forest", "boosting"])
    args = parser.parse_args()

    main(level=args.level, model=args.model)
