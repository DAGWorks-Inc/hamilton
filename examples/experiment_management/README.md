# Hamilton Experiment Manager

Add a hook to your Hamilton Driver to log runs and visualize artifacts and metadata! The server is built using FastAPI + FastUI allowing to easily integrate the server within your app or extend the UI.

## Features
- 📝 Track run metadata (config, inputs, code version, etc.)
- 📦 Generate directories to store your run artifacts
- 📡 Launch a local server to view and explore your experiments

## Installation
1. Find where Hamilton is installed using `pip show sf-hamilton`. Find the location entry:
    ```
    Name: sf-hamilton
    Version: 1.46.0
    ...
    Location: /home/username/.cache/pypoetry/virtualenvs/venv-Mv6TfRMQ-py3.10/lib/python3.10/site-packages
    ```
2. Go to the installation location with `cd /home/username/.cache/pypoetry/virtualenvs/venv-Mv6TfRMQ-py3.10/lib/python3.10/site-packages`
3. Go further down the directories using `cd examples/experiment_management`
4. Install the Hamilton Experiment Manager with `pip install -e .` (this install local files already bundled with Hamilton)
5. Check your installation by running `python -m hamilton_experiments`. It should start a FastAPI server:
    ```
    INFO:     Started server process [24113]
    INFO:     Waiting for application startup.
    INFO:     Application startup complete.
    INFO:     Uvicorn running on http://127.0.0.1:8123 (Press CTRL+C to quit)
    ```

## How to use the ExperimentTracker hook
The `ExperimentTracker` hook can be added to your Hamilton Driver definition to automatically log metadata about the run and materialized results.

1. Create the `ExperimentTracker` hook object:
    - `experiment_name`: name to organize related runs. Is used to create directories and displayed in the UI
    - `metadata_cache_path`: path where a small metadata cache will be created to store run metadata
    - `base_run_directory`: path under which subdirectories will be created to store artifacts
2. Create the Hamilton Driver and pass the `ExperimentTracker` to `with_adapters()` method
3. Define materializers for each artifacts you care about. The UI provides rich support for `parquet`, `csv`, and `json`.
    - ⚠ Make sure to use relative paths (ones that don't start with `/`) for artifacts to be stored in run directories.
4. Call `dr.materialize()` to launch run for which metadata and artifacts will be tracked.
5. (Optional) Use `dr.visualize_materialization()` to visualize the dataflow and set `output_file_path` with the run directory `tracker_hook.run_directory`

### Example
```python
from hamilton import driver
from hamilton_experiments.hook import ExperimentTracker

import my_functions  # <- your Hamilton module


# 1. create the hook
tracker_hook = ExperimentTracker(
    experiment_name="hello-world",
    base_directory="/path/to/experiments",
)

# 2. create driver with modules and hook
dr = (
    driver.Builder()
    .with_modules(my_functions)
    .with_adapters(tracker_hook)
    .build()
)

# 3. define materializers (absolute or relative path)
materializers = [
    # notice the relative paths (don't start with "/")
    to.json(
        id="model_performance__json",
        dependencies=["model_performance"],
        path="./model_performance.json",
    ),
    to.parquet(
        id="training_data__parquet",
        dependencies=["training_data"],
        path="./training_data.parquet",
    ),
]

# 4. launch run using `.materialize()`
dr.materialize(*materializers)

# 5. (optional) visualize materialization and store the figure
#               under the `tracker_hook.run_directory` path
dr.visualize_materialization(
    *materializers,
    output_file_path=f"{tracker_hook.run_directory}/dag",
)
```

## How to use the ExperimentServer
The ExperimentServer is a local FastAPI server that reads the run metadata cache and mounts the `base_directory` to view and explore results. The frontend uses FastUI to create a React interface from Python.

This server is starting point that one could extend to meets it specialized requirements or integrate in their own webapp.


1. Launch the experiment server with `python -m hamilton_experiments`
