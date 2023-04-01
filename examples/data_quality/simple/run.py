"""
This script runs a Hamilton DAG whose intent is to create some features for input to a model.
The model fitting steps are not represented here, just the feature ingestion and transformation logic.

Use this way if all your data can fit in memory, and multiprocessing would result in too much overhead (try using Ray
locally as a way to determine whether it is).
Don't bother scaling until you really need to!

To run:
> python run.py
"""

import logging
import sys

import data_loaders

# we need to tell hamilton where to load function definitions from
import feature_logic

from hamilton import driver

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    # passing in execution to help set up the right nodes for the DAG
    config = {"location": "Absenteeism_at_work.csv", "execution": "normal"}
    dr = driver.Driver(config, data_loaders, feature_logic)  # can pass in multiple modules
    # we need to specify what we want in the final dataframe.
    output_columns = [
        "age",
        "age_zero_mean_unit_variance",
        "has_children",
        "is_summer",
        "has_pet",
        "day_of_the_week_2",
        "day_of_the_week_3",
        "day_of_the_week_4",
        "day_of_the_week_5",
        "day_of_the_week_6",
        "seasons_1",
        "seasons_2",
        "seasons_3",
        "seasons_4",
        "absenteeism_time_in_hours",
    ]
    # To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
    # dr.visualize_execution(output_columns, './run', {"format": "png"})
    # dr.display_all_functions('./my_full_dag.dot')

    # let's create the dataframe!
    df = dr.execute(output_columns)
    print(df.head().to_string())
