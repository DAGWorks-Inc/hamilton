""" This file shows different usage pattern to integrate Hamilton with Apache Airflow

For the purpose of this example, we will read and write data from the Airflow
installation location (${AIRFLOW_HOME}/plugins/data).
ref: https://docs.astronomer.io/learn/airflow-passing-data-between-tasks
"""

import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

DEFAULT_DAG_PARAMS = dict(hamilton_config=dict(config_type="default_argument_from_dag_file"))


@dag(
    dag_id="hamilton-how-to",
    description="Showcase different Hamilton usage pattern",
    start_date=datetime(2023, 6, 18),
    params=DEFAULT_DAG_PARAMS,
)
def hamilton_how_to_dag():
    @task
    def basic():
        """This is the simplest Hamilton usage pattern. Data is loaded from an external
        source. Then, it's properly formatted to pass it to hamilton.driver.execute().
        The execution results can be handled freely.

        Tips:
        This approach benefits from the numerous Airflow integration for data loading
        and saving.

        0. import function modules
        1. load data from external source
        2. instantiate driver
        3. prepare inputs for driver
        4. execute driver
        5. handle results
        """
        import pandas as pd

        # 0. import function modules
        # feature_logic contains the data transformations;
        # find the file at hamilton/examples/airflow/plugins/functions_module/
        from function_modules import data_loaders, feature_logic

        from hamilton import driver

        # 1. load data from external source
        raw_df = pd.read_csv(f"{AIRFLOW_HOME}/plugins/data/Absenteeism_at_work.csv", sep=";")

        # 2. instantiate driver
        # the Python module `feature_logic` is passed to the driver
        dr = driver.Driver({}, feature_logic)

        # 3. prepare inputs for driver
        # hamilton.driver.execute() receives an an argument `inputs`, which is a dictionary
        # the keys should match the functions' arguments from the feature modules
        raw_df.columns = data_loaders._sanitize_columns(raw_df.columns)
        inputs = raw_df.to_dict(orient="series")

        final_vars = [
            "age_mean",
            "day_of_the_week_2",
            "day_of_the_week_3",
            "day_of_the_week_4",
            "day_of_the_week_5",
            "has_pet",
            "is_summer",
        ]

        # 4. execute driver
        results = dr.execute(final_vars=final_vars, inputs=inputs)

        # 5. handle results
        print(results.head())  # print to airflow logs for debugging

    @task
    def data_loaders():
        """Data is loaded using Hamilton's own data loading features. At execution,
        functions decorated with @load_from.* will load the specified source and take it as
        their node input. The @config.* decorators can be added to implement different
        behavior based on config (e.g., dev vs. prod).

        Tips:
        This pattern is useful when you want to execute the same Hamilton DAG from
        inside or outside Airflow / your production system. For example, you might want to
        orchestrate 100s of experiments in the cloud, but then rerun failed ones locally for
        debugging. This is easily achieved by using data_loaders.py on your local machine.

        It is possible to combine this pattern with directy data loading in Airflow.


        0. import function and data loading modules
        1. define config
        2. instantiate driver
        3. execute driver
        4. handle results
        """
        # 0. import function modules
        # data_loaders defines data loading behavior
        # find the file at hamilton/examples/airflow/plugins/functions_module/
        from function_modules import data_loaders, feature_logic

        from hamilton import driver

        # 1. define config
        # define the driver config with the key `location` and the value poiting to the csv
        # data source. This will be read as `path` value of the `@load_from.csv()` decorator
        # of `raw_data__base()` found in `data_loaders.py`
        config = {"location": f"{AIRFLOW_HOME}/plugins/data/Absenteeism_at_work.csv"}

        # 2. instantiate driver
        # both Python module `feature_logic` and `data_loaders` are passed to driver
        dr = driver.Driver(config, feature_logic, data_loaders)

        # 3. execute driver
        final_vars = [
            "age_mean",
            "day_of_the_week_2",
            "day_of_the_week_3",
            "day_of_the_week_4",
            "day_of_the_week_5",
            "has_pet",
            "is_summer",
        ]

        results = dr.execute(final_vars=final_vars)

        # 4. save results
        print(results.head())  # print to airflow logs for debugging

    @task
    def data_savers():
        """Data is saved using Hamilton's own data saving features. At execution,
        functions decorated with @save_to.* will save their result to the specified
        destination after successful execution. The @config.* decorators can be added to
        implement different behavior based on config (e.g., dev vs. prod).

        Tips:
        - can quickly change what is stored; useful for debugging
        - transportable code
        - would be more verbose otherwise

        """
        ...

    @task
    def config_from_airflow_dag_file(config=None):
        """value can be passed a Python object or a Jinja2 templated string"""

        print("config type: ", type(config))
        print("config content: ", config)

    @task
    def config_from_airflow_runtime():
        """"""
        context = get_current_context()
        print("context", context.keys())
        print("params", context["params"])
        dag_run = context["dag_run"]

        if not dag_run.conf.keys():
            print("dag_run.conf is empty. Did you trigger the DAG with config?")
            return

        config = dag_run.conf.get("hamilton_config")
        print("dag_run keys", dag_run.conf.keys())

        print("config type: ", type(config))
        print("config content: ", config)

    (
        basic()
        >> data_loaders()
        # load config from the file DEFAULT_ARGS dictionary
        >> config_from_airflow_dag_file(DEFAULT_DAG_PARAMS)
        # load config from the @dag(params=) argument using Jinja2 templated strings
        >> config_from_airflow_dag_file("{{params.hamilton_config}}")
        >> config_from_airflow_runtime()
    )


hamilton_how_to = hamilton_how_to_dag()


# def save_graph_visualization_to_xcom(hdriver) -> str:
#     """

#     """
#     import tempfile
#     from pathlib import Path

#     with tempfile.TemporaryDirectory() as tmpdir:
#         hdriver.visualize_execution(f"{tmpdir}/graph", render_kwargs={'view':False, "format": "svg"})
#         svg_string = Path(tmpdir, "graph.svg").read_text()

#     return svg_string
