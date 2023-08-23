import click

exec("from hamilton.plugins import h_spark")
import csv_data_loaders
import pyspark
import query_1
import query_8
import query_12

from hamilton import base, driver

QUERIES = {"query_1": query_1, "query_8": query_8, "query_12": query_12}


def run_query(query: str, data_dir: str, visualize: bool = True):
    """Runs the given query"""

    dr = (
        driver.Builder()
        .with_modules(QUERIES[query], csv_data_loaders)
        .with_adapter(base.DefaultAdapter())
        .build()
    )
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    if visualize:
        dr.visualize_execution(
            ["final_data"], f"./dag-{query}", {}, inputs={"data_dir": data_dir, "spark": spark}
        )
    df = dr.execute(["final_data"], inputs={"data_dir": data_dir, "spark": spark})["final_data"]
    print(df)


@click.command()
@click.option("--data-dir", type=str, help="Base directory for data", required=True)
@click.option(
    "--which", type=click.Choice(list(QUERIES.keys())), help="Which query to run", required=True
)
@click.option(
    "--visualize",
    type=bool,
    help="Whether to visualize the execution",
    is_flag=True,
)
def run_tpch_query(data_dir: str, which: str, visualize: bool):
    """Placeholder function for running TPCH query"""
    # Place logic here for running the TPCH query with the given 'which' value
    click.echo(f"Running TPCH query: {which}")
    run_query(which, data_dir, visualize)


if __name__ == "__main__":
    run_tpch_query()
