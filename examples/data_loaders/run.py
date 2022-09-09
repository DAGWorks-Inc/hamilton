import click
import load_data_mock
import prep_data

import hamilton.driver
from examples.data_loaders import load_data_csv, load_data_duckdb


@click.group()
def main():
    pass


VARS = [
    "total_signups",
    "total_churn",
    "total_marketing_spend",
    "acquisition_cost",
    "twitter_spend_smoothed",
    "facebook_spend_smoothed",
    "radio_spend_smoothed",
    "tv_spend_smoothed",
    "billboards_spend_smoothed",
    "youtube_spend_smoothed",
]


@main.command()
def duckdb():
    driver = hamilton.driver.Driver(
        {"db_path": "./test_data/database.duckdb"}, load_data_duckdb, prep_data
    )
    print(driver.execute(VARS))


@main.command()
def csv():
    driver = hamilton.driver.Driver({"db_path": "test_data"}, load_data_csv, prep_data)
    print(driver.execute(VARS))


@main.command()
def mock():
    driver = hamilton.driver.Driver({}, load_data_mock, prep_data)
    print(driver.execute(VARS))


if __name__ == "__main__":
    main()
