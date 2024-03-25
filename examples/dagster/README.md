# Dagster + Hamilton

This repository compares how to build dataflows with macro orchestrator Dagster and the micro orchestrator Hamilton.

> see the [side-by-side comparison](https://hamilton.dagworks.io/en/latest/code-comparisons/dagster/) in the Hamilton documentation

## Content
- `dagster_code/` includes code from the [Dagster tutorial](https://docs.dagster.io/tutorial) to load data and compute statistics from the website [HackerNews](https://news.ycombinator.com/).
- `hamilton_code/` is a refactor of `dagster_tutorial/` using the Hamilton framework.

Each directory contains instructions on how to run the code. We suggest going through the Dagster code first, then read the Hamilton refactor.

## Setup
1. Create a virtual environment and activate it
    ```console
    python -m venv venv && . venv/bin/active
    ```

2. Install requirements for both Dagster and Hamilton
    ```console
    pip install -r requirements.txt
    ```

3. Dagster-specific instructions are found under `dagster_code/`
