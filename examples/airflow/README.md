# Airflow setup
1. Write `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`
2. Write `docker compose up --build`
3. Connect via `http://localhost:8080`. The default username is `airflow` and the password is `airflow`

# Motivation
1. **Simplify Airflow DAG**. Respect the ethos of Airflow:
```
Make your DAG generate simpler structure. Every task dependency adds additional processing overhead for scheduling and execution. The DAG that has simple linear structure A -> B -> C will experience less delays in task scheduling than DAG that has a deeply nested tree structure with exponentially growing number of depending tasks for example. If you can make your DAGs more linear - where at single point in execution there are as few potential candidates to run among the tasks, this will likely improve overall scheduling performance.
```
 [reference](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#communication)


2. **Separate infrastructure from data science concerns**. Airflow is responsible for orchestrating tasks and is typically owned handled by data engineers. Hamilton is responsible for authoring clean, reusable, and maintainable data transformations and is typically owned by data scientists. By integrating both tools, these two personas/teams have clearer ownership and version control over their codebases, and it can promote reusability of both Airflow and Hamilton code. Notably, one can replace dynamic Airflow DAG (generated via config) by static Airflow DAG and let Hamilton handle the dynamic data transformation requirements. This better separate the consistent Airflow infrastructure pipeline from the project specific Hamilton data transforms. On the opposite, Hamilton data transforms can be reused in Airflow pipelines to move data to power different initiatives (dashboard, app, API, etc.) greatly improving consistency.
3. **Write efficient Python code efficiently**. With Python being the 3rd most popular programming language in 2023, most data professionals should be able to pick up Airflow and Hamilton quickly. However, production systems are composed of multiple services (database, datawarehouse, compute cluster, cache, serverless functions, etc.) most of which are SQL-based or have a Python SDK. This puts strains on engineers that need to learn and maintain this sprawling codebase. For orchestration, Airflow **providers** standardize interactions between the airflow DAG and external systems using the Python language ([see providers](https://registry.astronomer.io/providers)). For data transformation, Hamilton **graph adapters** can automatically convert pandas code to production-grade computation engines such as Ray, Dask, Spark, Pandas on Spark (Koalas), Async Python ([see graph adapters (experimental)](https://hamilton.readthedocs.io/en/latest/reference/api-reference/graph-adapters.html)) These graph adapters automatically provide benefits such as result caching, GPU computing or out-of-core computation.


# Notes

- example uses Airflow default Docker with Python 3.7
- Currently, airflow doesn't support Python 3.11 and must be installed via pip
- changed a line in docker compose to add ./includes directory
- module import from includes.module ([reference](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html))
- docker file must include graphviz backend
- current driver.display_all_functions() is needs write access to render a file
- store DAG visualization artifacts ([reference](https://github.com/apache/airflow/issues/15485)); ([custom implementation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html#implementing-a-custom-file-task-handler))
- see how @save_to combines with airflow complex worker setup for task (it shouldn't kill Python until the full Python process is done)
- point to specific function definition in docstrings
- Performance: Airflow can have significant task overhead that Hamilton helps reduce. Your Airflow setup might have existing computation optimization (workers, queues, auto-scaling, etc.).
- Separation of concerns: separation between infra and ds is reduced if infra engineer need to dig into Hamilton codebase or internals to optimize production performance.
