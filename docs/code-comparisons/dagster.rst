=========
Dagster
=========

Here are some code snippets to compare the macro orchestrator Dagster to the micro orchestrator Hamilton. Hamilton can run inside Dagster, but you wouldn't run Dagster inside Hamilton.

While the two have different scope, there's a lot of overlap between the two both in terms of functionality and API. Indeed, Dagster's software-defined assets introduced in 2022 matches Hamilton's declarative approach and should feel familiar to users of either.


------
TL;DR
------

.. list-table::
    :widths: 24 39 39
    :header-rows: 1

    * - Trait
      - Hamilton
      - Dagster
    * - Declarative API
      - ✅
      - ✅
    * - Dependencies
      - Lightweight library with minimal dependencies (``numpy``, ``pandas``, ``typing_inspect``). Minimizes dependency conflicts.
      - Heavier framework/system with several dependencies (``pydantic``, ``sqlalchemy``, ``requests``, ``Jinja2``, ``protobuf``). ``urllib3`` on which depends ``requests`` introduced breaking changes several times and ``pydantic`` v1 and v2 are incompatible.
    * - Macro orchestration
      - DIY or in tandem with Dagster, Airflow, Prefect, Metaflow, etc.
      - Includes: manual, schedules, sensors, conditional execution
    * - Micro orchestration (i.e., ``dbt``, ``LangChain``)
      - Can run anywhere (locally, macro orchestrator, `FastAPI <https://hamilton.dagworks.io/en/latest/integrations/fastapi/>`_, `Streamlit <https://hamilton.dagworks.io/en/latest/integrations/streamlit/>`_, pyodide, etc.)
      - ❌
    * - LLM applications
      - Well-suited for `LLM applications <https://blog.dagworks.io/p/retrieval-augmented-generation-reference-arch>`_ since it's a micro orchestration framework.
      - ❌
    * - Lineage
      - Fine-grained / column-level lineage. Includes `utilities to explore lineage <https://hamilton.dagworks.io/en/latest/how-tos/use-hamilton-for-lineage/>`_.
      - Coarser operations to reduce orchestration and I/O overhead.
    * - Visualization
      - `View the dataflow and produce visual artifacts <https://hamilton.dagworks.io/en/latest/concepts/visualization/>`_. Configurable and supports extensive custom styling.
      - Export Daster UI in ``.svg``. No styling.
    * - Run tracking
      - `DAGWorks <https://docs.dagworks.io/capabilities>`_ (premium)
      - Dagster UI
    * - Experiment Managers
      - Has an `experiment manager plugin <https://blog.dagworks.io/p/building-a-lightweight-experiment>`_
      - ❌
    * - Materializers
      - `Data Savers & Loaders <https://hamilton.dagworks.io/en/latest/concepts/materialization/>`_
      - `IO Managers <https://docs.dagster.io/_apidocs/io-managers>`_
    * - Data validation
      - `Native validators and pandera plugin <https://hamilton.dagworks.io/en/latest/how-tos/run-data-quality-checks/>`_ 
      - `Asset checks <https://docs.dagster.io/_apidocs/asset-checks>`_ (experimental), `pandera integration <https://docs.dagster.io/integrations/pandera>`_
    * - Versioning
      - Nodes and dataflow versions derived from code. The `DiskCacheAdapter <https://docs.dagster.io/concepts/assets/software-defined-assets#asset-code-versions>`_ automatically recomputes values for code **or** data changes.
      - Manual `Asset code versioning <https://docs.dagster.io/concepts/assets/software-defined-assets#asset-code-versions>`_ to trigger re-materialization.
    * - In-memory Execution
      - Default
      - `Materialize in-memory <https://docs.dagster.io/_apidocs/io-managers>`_
    * - Task-based Execution
      - `TaskBasedExecutor <https://docs.dagster.io/_apidocs/io-managers>`_
      - Default
    * - Dynamic branching
      - `Parallelizable/Collect <https://hamilton.dagworks.io/en/latest/concepts/parallel-task/>`_
      - `Mapping/Collect <https://docs.dagster.io/_apidocs/dynamic>`_
    * - Hooks
      - `Lifecycle hooks <https://hamilton.dagworks.io/en/latest/reference/lifecycle-hooks/>`_ (easier to extend)
      - `Op Hooks <https://docs.dagster.io/concepts/ops-jobs-graphs/op-hooks#op-hooks>`_
    * - Plugins
      - `Spark <https://blog.dagworks.io/p/expressing-pyspark-transformations>`_, Dask, Ray, `Datadog <https://hamilton.dagworks.io/en/latest/reference/lifecycle-hooks/DDOGTracer/>`_, polars, pandera, and more (Hamilton is less restrictive and easier to extend)
      - `Spark, Dask, polars, pandera, Databricks, Snowflake, Great Expections, and more <https://docs.dagster.io/integrations>`_  (Dagster integrations are more involved to develop)
    * - Interactive Development
      - `Jupyter Magic <https://hamilton.dagworks.io/en/latest/how-tos/use-in-jupyter-notebook/#use-hamilton-jupyter-magic>`_, `VSCode extension <https://marketplace.visualstudio.com/items?itemName=ThierryJean.hamilton>`_
      - ❌


----------------------
Dataflow
----------------------

.. table:: HackerNews top stories
   :align: left

   +------------------------------------------------------------+----------------------------------------------------------+
   | Hamilton                                                   | Dagster                                                  |
   +============================================================+==========================================================+
   | .. literalinclude:: _dagster_snippets/hamilton_dataflow.py | .. literalinclude:: _dagster_snippets/dagster_dataflow.py| 
   |                                                            |                                                          |
   +------------------------------------------------------------+----------------------------------------------------------+
   | .. image:: _dagster_snippets/hamilton_dataflow.png         | .. image:: _dagster_snippets/dagster_dataflow.png        |
   |                                                            |                                                          |
   +------------------------------------------------------------+----------------------------------------------------------+

.. list-table:: Key points
    :widths: 24 39 39
    :header-rows: 1

    * - Trait
      - Hamilton
      - Dagster
    * - Define operations
      - Uses the native Python function signature. The dataflow is assembled based on function/parameter names and type annotations.
      - Uses the ``@asset`` decorator to transform function in operations and specify dependencies by passing functions
    * - Execution
      - A Python process walks the graph, computes nodes and passes values between functions in-memory. Task-based executors are also available.
      - A central orchestrator launches jobs and computes data assets based on their dependencies.
    * - Lineage
      - Favors granular operations and fine-grained lineage. For example, ``most_frequent_words()`` operates on a single column and the ``top_25_words_plot`` is its own function. 
      - Favors chunking dataflow into meaningful assets to reduce the orchestration and I/O overhead per operation.
    * - Documentation
      - Uses the native Python docstrings. Further metadata can be added using the ``@tag`` decorator. 
      - Uses ``MaterializeResult`` to store metadata.
