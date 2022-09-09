import click
import load_data_mock
import prep_data

import hamilton.driver
from examples.data_loaders import load_data_csv, load_data_duckdb


@click.group()
def main():
    pass


@main.command()
def duckdb():
    driver = hamilton.driver.Driver(
        {"db_path": "./test_data/database.duckdb"}, load_data_duckdb, prep_data
    )
    vars = driver.list_available_variables()
    # print(driver.raw_execute([var.name for var in vars]))
    out = driver.execute([var.name for var in vars if "smoothed" in var.name])
    print(out)


@main.command()
def csv():
    driver = hamilton.driver.Driver({"db_path": "test_data"}, load_data_csv, prep_data)
    vars = driver.list_available_variables()
    print(driver.execute([var.name for var in vars]))


@main.command()
def mock():
    driver = hamilton.driver.Driver({}, load_data_mock, prep_data)
    vars = driver.list_available_variables()
    print(
        driver.raw_execute(
            [var.name for var in vars], inputs={"start_date": "20220801", "end_date": "20220901"}
        )
    )


if __name__ == "__main__":
    main()
