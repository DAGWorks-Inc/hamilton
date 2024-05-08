# Hamilton UI

Hamilton comes with a fully open-source UI that can be run both for local deployment and on a remote server. The UI consists of the following features:

- Dataflow (DAG) viewer. Interactively explore your code and visualize lineage.

- Execution telemetry. Consult the metadata history on dataflow execution.

- Artifact catalog. Browse versioned results of node execution.

- Project explorer. Curate dataflows related to a project and view diffs between versions.


## Installation

1. Install the Hamilton UI container [by following these instructions](https://hamilton.dagworks.io/en/latest/concepts/ui/#starting-the-ui).

2. Through the UI, create a new project and note its `project_id` (it will be `1` if it's your first project)

3. Create a virtual environment and activate it
    ```console
    python -m venv venv && . venv/bin/active
    ```

4. Install requirements, including the Hamilton SDK (`sf-hamilton[sdk]`)
    ```console
    pip install -r requirements.txt
    ```

5. Configure the `HamiltonTracker` and it to your `Driver` definition.

  ```python
  from hamilton_sdk.adapters import HamiltonTracker
  from hamilton import driver

  tracker = HamiltonTracker(
    project_id=1,  # modify this as needed
    username="username@domain.com", # modify this as needed
    dag_name="my_version_of_the_dag",
    tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"}
  )
  dr = (
    driver.Builder()
      .with_modules(*your_modules)
      .with_adapters(tracker)
      .build()
  )
  # use `dr.execute() or dr.materialize() to execute code
  ```

6. Go to `run.py` in this directory and modify the `project_id` and `username` with the values found in the Hamilton UI.

7. Execute your dataflow with

  ```console
  python run.py
  ```
