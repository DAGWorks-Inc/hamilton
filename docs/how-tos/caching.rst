========
Caching
========

In Hamilton, the term *caching* broadly refers to "reusing results from previous executions to skip redundant computation". When caching is enabled, node results are stored in a **result store** and where they can be retrieved by subsequent runs.

.. seealso::

    For a broader overview of how caching works, see the :doc:`/concepts/caching` Concepts page .

    For implementation details, see the :doc:`/reference/caching/caching-logic` Reference page .


Use cases
-----------

Caching is beneficial for almost all use cases, and it's likely a good idea to have it on. The next sections present a illustrative scenarios.


Iterative notebook development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You are developing a dataflow in a notebook (analytics, ETL, ML pipeline, LLM workflow, etc.). You want to explore and iterate quickly over different transformation steps. For example:

.. code-block:: python

    import pandas as pd
    from sklearn.linear import LinearRegression

    def raw_data(file_path: str) -> pd.DataFrame:
        return pd.read_parquet(file_path)

    def cleaned_data(raw_data: pd.DataFrame) -> pd.DataFrame:
        cleaned_df = ...  # fill nulls, filter rows, rename columns
        return cleaned_df

    def statistics(cleaned_data: pd.DataFrame) -> dict:
        return cleaned_data.describe().to_dict()

    # def predictor(cleaned_data: pd.DataFrame) -> LinearRegression
    #    ...


.. TODO add DAG viz
.. TODO add notebook link


When adding a node (e.g., ``predictor``), only this new node is executed. After editing the code of an intermediary node (e.g., modifying ``cleaned_data``), Hamilton will determine if downstream nodes need to be re-executed (e.g., ``statistics`` and ``predictor``).

.. note::

    Caching pairs nicely with the notebook extension. Try this example notebook.


**Benefits**

- Faster debugging by avoiding redundant upstream computation after changing code.

- Persist your notebooks. Using the cache allows you to restart the notebook kernel or shutdown your computer for the day, and be able to resume from where you left next time you execute the dataflow.


Optimize ML and LLM dataflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You are optimizing a predictive dataflow (forecast, RAG, classification, recommender systems, etc.) and want to try a large number of permutation of processing steps. This is effectively "hyperparameter tuning" over the dataflow rather than only the predictor.

.. code-block:: python

    ...


Caching allows to automatically reuse results between optimization iterations. Because results are stored "node by node", caching remains effective when reshaping the dataflow (e.g., adding or removing a step with ``@config.when``).

**Benefits**

- Automatically reduce wasteful computation (cloud costs, GPU time) and API calls. Save both time and money.

- Caching removes the need for the error-prone process of manually saving and loading "checkpoints" for optimization. Faulty manual caching would make results unreliable and untrustworthy.

- Strong lineage of "what data" was used in each run is stored in the cache metadata allowing you to inspect and reuse intermediary artifacts.

- Paired with the Hamilton UI and the MLFlowTracker, this creates an unparalleled experiment tracking experience.


Analytics and BI
~~~~~~~~~~~~~~~~~~

You are building an analytics product or dashboard that allows users to slice and dice data interactively (filters, aggregations, pivot, etc.).

Caching allows to store expensive computations (e.g., pivoting a table) and reuse them to reduce latency of the user interface. Servers typically have a lot of unused disk space, so this can be a more cost-effective approach than using memory-caching like Redis.
