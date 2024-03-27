# Dagster

This project is adapted from the official [Dagster tutorial](https://docs.dagster.io/tutorial).

## File structure
- `tutorial/` is a Python module that contains our Dagster project. It needs to be installed in our Python environment.
- `pyproject.toml` and `setup.py` define how to install the `tutorial/` Dagster project.
- `tutorial/assets.py` defines the data assets to compute and materialize.
- `tutorial/__init__.py` register the data assets, jobs, and resources for the orchestrator.
- `tutorial/resources/` contains informations to connect to external resources and API.

## Instructions
1. Install the Dagster project as a Python module
    ```console
    pip install -e .
    ```
2. Launch the Dagster UI
    ```console
    dagster dev
    ```
3. Access Dagster UI via the generated link (default: http://127.0.0.1:3000)
