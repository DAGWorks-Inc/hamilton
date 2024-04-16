import pandas as pd
from app import functions

from hamilton import driver

if __name__ == "__main__":

    df = pd.read_csv("/opt/ml/processing/input/data/input_table.csv")

    dr = driver.Driver({}, functions)

    inputs = {"input_table": df}

    output_columns = [
        "spend",
        "signups",
        "avg_3wk_spend",
        "spend_per_signup",
        "spend_zero_mean_unit_variance",
    ]

    # DAG visualization
    dot = dr.visualize_execution(final_vars=output_columns, inputs=inputs)
    with open("/opt/ml/processing/output/dag_visualization.svg", "wb") as svg_out:
        svg_out.write(dot.pipe(format="svg"))

    # DAG execution
    df_result = dr.execute(output_columns, inputs=inputs)
    df_result.to_csv("/opt/ml/processing/output/output_table.csv")
