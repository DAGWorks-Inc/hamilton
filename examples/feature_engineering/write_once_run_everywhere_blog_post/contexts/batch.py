import datetime

import click
from components import aggregations, data_loaders, features, joins, model

from hamilton import driver


@click.group()
def cli():
    pass


def _create_driver() -> driver.Driver:
    return driver.Driver({"mode": "batch"}, aggregations, data_loaders, joins, features, model)


def _get_inputs() -> dict:
    return {
        "client_login_db": "login_data",
        "client_login_table": "client_logins",
        "survey_results_db": "survey_data",
        "survey_results_table": "survey_results",
        "execution_time": datetime.datetime.now(),
    }


@cli.command()
def run():
    """This command will run the ETL, and print it out to the terminal"""
    dr = _create_driver()
    df = dr.execute(["predictions"], inputs=_get_inputs())
    print(df)


@cli.command()
@click.option("--output-file", default="./out", help="Output file to write to")
def visualize(output_file: str):
    """This command will visualize execution"""
    dr = _create_driver()
    return dr.visualize_execution(
        ["predictions"], output_file, {"format": "png"}, inputs=_get_inputs()
    )


if __name__ == "__main__":
    cli()
