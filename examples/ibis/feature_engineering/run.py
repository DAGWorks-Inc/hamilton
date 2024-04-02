from hamilton import driver
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


def main(level: str):
    if level == "column":
        import column_dataflow

        feature_dataflow = column_dataflow
    elif level == "table":
        import table_dataflow

        feature_dataflow = table_dataflow
    else:
        raise ValueError("`level` must be in ['column', 'table']")

    # build the Driver from modules
    dr = driver.Builder().with_modules(feature_dataflow).with_adapters(ProgressBar()).build()

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
    )

    res = dr.execute(["feature_set"], inputs=inputs)
    view_expression(res["feature_set"], filename="ibis_feature_set", format="png")

    print("Dataflow result keys: ", list(res.keys()))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--level", choices=["column", "table"], default="table")
    args = parser.parse_args()

    print(f"Running dataflow at {args.level} level")
    main(level=args.level)
