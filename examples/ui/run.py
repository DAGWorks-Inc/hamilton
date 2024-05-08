import data_loading
import features
import model_pipeline
import sets
from hamilton_sdk.adapters import HamiltonTracker

from hamilton import driver

tracker = HamiltonTracker(
    project_id="PROJECT_ID",  # modify this as needed
    username="USERNAME",  # modify this as needed
    dag_name="my_version_of_the_dag",
    tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
)

dr = (
    driver.Builder()
    .with_modules(data_loading, features, sets, model_pipeline)
    .with_adapters(tracker)
    .build()
)

inputs = {
    "location": "../lineage/data/train.csv",
    "index_col": "passengerid",
    "target_col": "survived",
    "random_state": 42,
    "max_depth": None,
    "validation_size_fraction": 0.33,
}
dr.execute(
    ["fit_random_forest", "X_test", "y_test", "titanic_data", "encoders"],
    inputs=inputs,
)
