# Comparison to Other Frameworks

There are a lot of MLOps frameworks out there, especially in the pipeline space. This should help you figure out when to
use Hamilton with another framework, or in place of a framework, or when to use another framework altogether.

Let's go over some groups of "competitive" or "complimentary" products. For a basic overview,
see the product matrix on the [homepage](main.md).

## Orchestration Systems
Examples include:
- [Airflow](https://airflow.apache.org/)
- [Metaflow](https://github.com/Netflix/metaflow)
- [Luigi](https://github.com/spotify/luigi)
- [dbt](https://www.getdbt.com/)

Hamilton is not, in itself a macro, i.e. high level, task orchestration system. While it has rudimentary capabilities
to orchestrate (run locally), and the DAG abstraction is very powerful, it does not provision compute,
or schedule long-running jobs. It tends to work well in conjunction with them. Hamilton provides the capabilities
of fine-grained lineage, highly readable code, and self-documenting pipelines, which many of these systems lack.

Hamilton can be used within any python
orchestration system in the following ways:

1. _Hamilton DAGs can be called within orchestration system tasks._
See the [Hamilton + Metaflow] example: https://github.com/outerbounds/hamilton-metaflow. The integration is generally trivial -- all you have to do
is call out to the hamilton library within your task. If your orchestrator supports python, then you're good to go. Some pseudocode (if your orchestrator handles scripts like airflow):

    ```python
    #my_task.py
    import hamilton
    import my_transformations
    dr = hamilton.driver.Driver({}, my_functions)
    output = dr.execute(['final_var'], inputs=...)
    do_something_with(output)
    ```
2. _Hamilton DAGs can be broken up to run as components within an orchestration system._
With the ability to include [overrides](concepts/driver-capabilities.rst),
you can run the DAG on each task, overloading the outputs of the last task + any static inputs/configuration, and pass it into the next task. This is more
of a manual/power-user feature. Some pseudocode:

    ```python
    #my_task.py
    import hamilton
    import my_functions
    prior_inputs = load_relevant_task_results()
    desired_outputs = ['final_var_1', 'final_var_2']
    inputs = my_inputs
    dr = hamilton.driver.Driver({}, my_functions)
    output = dr.execute(
       desired_outputs,
       inputs=inputs,
       overrides=prior_inputs)
    save_for_later(output)
    ```

Again this is in a script-based orchestrator (like airflow). This should be easy to adopt in
a more function-based orchestrator. For a flytekit-like orchestrator (that utilizes functions and stores data for you),
you can just pass the function arguments in as overrides!

## Feature Stores

Examples include:
- [Hopsworks](https://www.hopsworks.ai/)
- [Feast](https://feast.dev/)
- [Tecton](https://tecton.ai/)

One can think of Hamilton as a "feature store as code". While it does not provide all the capabilities of a standard feature
store, it provides a source of truth for the code that generated the features, and can be run in a portable
method. *So*, if your desire is just to be able to run the same code in different environments, and have an online/offline
store of features, you can use hamilton both to save the features offline, and generate features online on the fly.

See the [feature engineering example](how-tos/use-for-feature-engineering.rst) for more possibilities.

Note that in small cases, you probably don't need a true feature store -- recomputing derived features in an ETL
and online can be very efficient, as long as you have some database to look features up (or have them passed in).

Also note that joins and aggregations can get tricky. We often recommend using polymorphic function
definition (`@config.when`) to either load up the non-online-friendly features from a feature store or do an
external lookup to simulate an online join.

This field is actively developing, and we expect Hamilton to play a prominent role in the way future stores
work in the future.


## Data Science Ecosystems/ML platforms
Examples include:
- [Kedro](https://kedro.org/)
- [MLflow](https://mlflow.org/)
- [Domino Data Labs](https://www.dominodatalab.com/)

And many others. We've kind of grouped a whole suite of platforms into the same bucket here. These
tend to have a lot of capabilities all related to ML. Hamilton can be run within these platforms,
generate features for them to read, save models to their registry, and load models from their registry
for inference. For example, you could imagine the following pseudocode for a Hamilton DAG that computes
a model, and then delegates to saving the model in a registry.

```python
# training.py

def model(training_data: pd.DataFrame) -> Model:
    return Model.train(training_data)

# run.py
import training
import hamilton
from hamilton import base

dr = hamilton.driver.Driver(config={}, training, adapter=base.DefaultAdapter())
model = dr.execute(["model"], ...)
save_model_to_registry(model, ...) # With any extra metadata
```

## Python Compute/parallelism Systems

Examples include:
- [pandas](https://pandas.pydata.org/)
- [dask](https://www.dask.org/)
- [ray](https://ray.io/)
- [modin](https://github.com/modin-project/modin)
- [pyspark](https://spark.apache.org/docs/latest/api/python/)
- [pandas-on-spark](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html)
- [polars](https://www.pola.rs/)
- [duckdb](https://duckdb.org/)

These all provide capabilities to either (a) express and execute computation over datasets in python or (b)
parallelize it. Often both. Hamilton has a variety of integrations with these systems. The basics is that Hamilton
can make use of these systems to execute the DAG using the [GraphAdapter](reference/graph-adapters/index.rst) abstraction.

Hamilton also has a variety of plugins that further integrate with these systems. See the [hamilton without pandas](how-tos/use-without-pandas.rst) example for more details.
