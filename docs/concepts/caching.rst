========
Caching
========

In Hamilton, the term *caching* broadly refers to "reusing results from previous executions to skip redundant computation". When caching is enabled, node results are stored in a **result store** and where they can be retrieved by subsequent runs.

By default, results are stored based on the **code** that defines the node and its input **data** by default. Consequently, running a node with the "same code + same input data" multiple times should lead to a single execution.

.. important::

    Caching being a new core feature, please reach out via GitHub or Slack for requests or issues.

Basics
-------

To get started, add ``.with_cache()`` to the ``Builder()``. This will create the subdirectory ``hamilton_cache/`` in the current directory. Calling ``Driver.execute()`` will execute the dataflow as usual, but it will also store **metadata** and **results** under ``hamilton_cache/``. When calling ``.execute()`` a second time, the ``Driver`` will use the **metadata** to determine if **results** can be loaded, effectively skipping execution!

.. code-block:: python

    from hamilton import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_module(my_dataflow)
        .with_cache()
        .build()
    )

    dr.execute([...])


.. _cache-result-format:

Cached result format
---------------------

By default, results are stored using the ``pickle`` format. It's a convenient default because it's part of the Python standard library, but it also `comes with caveats <https://grantjenks.com/docs/diskcache/tutorial.html#caveats>`_. To specify another file format to cache the result (``JSON``, ``CSV``, ``Parquet``, etc.), you can use the ``@cache`` function modifier and its ``format`` parameter.

The following code shows the node ``raw_data`` will be cached to ``pickle`` (the default), ``clean_dataset`` will be saved to ``parquet``, and ``statistics`` will be stored as ``json``. While cached results aren't meant to be "durable", these formats can be more reliable and reusable for other purposes.

.. code-block:: python

    # my_dataflow.py
    import pandas as pd
    from hamilton.function_modifiers import cache

    def raw_data(path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    @cache(format="parquet")
    def clean_dataset(raw_data: pd.DataFrame) -> pd.DataFrame:
        raw_data = raw_data.fillna(0)
        return raw_data

    @cache(format="json")
    def statistics(clean_dataset: pd.DataFrame) -> dict:
        return ...


.. code-block:: python

    import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_modules(my_dataflow)
        .with_cache()
        .buid()
    )

    # first execution will product a ``parquet`` file for  ``clean_dataset``
    # and a ``json`` file for ``statistics``
    dr.execute(["statistics"])
    # second execution will use these parquet and json files when loading results
    dr.execute(["statistics"])

.. note::

    Internally, this uses :doc:`Materializers </concepts/materialization>`

Caching behavior
-----------------

The default **caching behavior** aims to be easy to use and facilitate iterative development. However, in production and specific scenarios, you may need more control over caching. The caching behavior can be set node-by-node as one of the following:

1. **Default**: Try to retrieve results from cache instead of executing the node. Node result and metadata are stored.

2. **Recompute**: Always execute the node / never retrieve from cache. Result and metadata are stored.

3. **Disable**: Don't try to retrieve from cache and don't store anything, as if caching wasn't enabled. Nodes depending on it will miss metadata for cache retrieval, forcing their re-execution. Useful for disabling caching in parts of the dataflow.

4. **Ignore**: Similar to **Disable**, but downstream nodes will ignore the missing metadata and can successfully retrieve results. Useful to ignore "irrelevant" nodes that should impact the results (e.g., credentials, API clients, database connections).

All nodes without an explicit caching behavior will be set to ``Default``.

.. seealso::

    Learn more in the :doc:`/reference/caching/caching-logic` reference section.


Setting caching behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The caching behavior can be specified at the node-level via the ``@cache`` function modifier or at the builder-level via ``.with_cache(...)`` arguments. Note that the behavior specified by the ``Builder`` will override the behavior from ``@cache`` since it's closer to execution.

via ``@cache``
~~~~~~~~~~~~~~~

Below, we set ``raw_data`` to ``RECOMPUTE`` because the file it loads data from may change between executions. After executing and versioning the result of ``raw_data``, if the data didn't change from previous execution, we'll be able to retrieve ``clean_dataset`` and ``statistics`` from cache.

.. code-block:: python

    # my_dataflow.py
    import pandas as pd
    from hamilton.function_modifiers import cache

    @cache(behavior="recompute")
    def raw_data(path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    def clean_dataset(raw_data: pd.DataFrame) -> pd.DataFrame:
        raw_data = raw_data.fillna(0)
        return raw_data

    def statistics(clean_dataset: pd.DataFrame) -> dict:
        return ...

via ``Builder().with_cache()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Equivalently, we could set this behavior via the ``Builder``. You can pass a list of node names to the keyword arguments ``recompute``, ``ignore``, and ``disable``. Using ``True`` to enable that behavior for all nodes. For example, using ``recompute=True`` will force execution of all nodes and store their results in cache. Having ``disable=True`` is equivalent to not having the ``.with_cache()`` clause.

.. code-block:: python

    from hamilton import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_modules(my_dataflow)
        .with_cache(recompute=["raw_data"])
        .build()
    )


Cache logging
---------------

You can monitor and log the cache behavior by retrieving the module's logger. Then, ``Driver.execute()`` will log events indicating metadata retrieval, result retrieval, node execution, etc. Setting the log level to ``logging.INFO`` will only display ``GET_RESULT`` and ``EXECUTE_NODE`` events while the level ``logging.DEBUG`` will log all events


.. code-block:: python

    # my_dataflow.py
    def raw_data() -> pd.DataFrame:
        return pd.DataFrame(...)

    def processed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
        """cleanup the raw data"""

    def amount_per_country(processed_data: pd.DataFrame) -> dict:
        """Compute aggregations and statistics"""


.. code-block:: python

    import logging

    from hamilton import driver
    import dataflow

    logger = logging.getLogger("hamilton.lifecycle.caching")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    dr = (
        driver.Builder()
        .with_modules(dataflow)
        .with_cache()
        .build()
    )

    # execute twice
    dr.execute(["amount_per_country"])
    dr.execute(["amount_per_country"])


The logs follow the structure ``{node_name}::{task_id}::{actor}::{event_type}::{message}``, omitting empty sections (e.g., ``task_id``, ``message``)


.. code-block:: console

    # first execution INFO logs
    raw_data::adapter::execute_node
    processed_data::adapter::execute_node
    amount_per_country::adapter::execute_node

    # second execution INFO logs
    raw_data::result_store::get_result::hit
    processed_data::result_store::get_result::hit
    amount_per_country::result_store::get_result::hit


.. _caching-structured-logs:

Structured logs
~~~~~~~~~~~~~~~~

You can also inspect the logs programmatically via the ``Driver.cache.logs()`` method. This will returns the logs for all executions of this ``Driver``. This method also supports the keyword argument ``level`` with values ``"info"`` (default) and ``"debug"``, similar to the ``logging`` level. For example:

.. code-block:: python

    dr.execute(...)
    dr.cache.logs(level="info")


Requesting ``Driver.cache.logs()`` will return a dictionary with ``run_id`` as key and list of ``CachingEvent`` as values ``{run_id: List[CachingEvent]}``. This is useful for comparing run and verify nodes were properly executed or retrieved.


.. code-block:: python

    {
        '548f4350-c7e4-449e-b5e9-46a5213f8978': [
            CachingEvent(...),
            CachingEvent(...),
            CachingEvent(...)
        ],
        '560940a7-19ab-4243-ba36-968bfd33b9c4': [
            CachingEvent(...),
            CachingEvent(...),
            CachingEvent(...)
        ]
    }


Setting the keyword argument ``run_id`` allows to retrieve a single run and reshape the logs to be keyed by ``node_name``, resulting in ``{node_name: List[CachingEvent]}``. The following snippets shows how the retrieve logs from the latest execution using the ``Driver.cache.run_id``:


.. code-block:: python

    dr.execute(...)
    dr.cache.logs(dr.cache.run_id, level="debug")


.. code-block:: python

    {
        'raw_data': [
            CachingEvent(...),
            CachingEvent(...),
            ...
        ],
        'processed_data': [
            CachingEvent(...),
            CachingEvent(...),
            ...
        ],
        'amount_per_country': [
            CachingEvent(...),
            CachingEvent(...),
            ...
        ]
    }


.. note::

    If your dataflow includes ``Parallelizable/Collect`` constructs and you're requesting ``Driver.cache.logs(run_id=...)``, the logs will have a slightly different shape. Nodes outside parallel branches will have ``{node_name: List[CachingEvent]}`` while the ``Parallelizable`` node and all the downstream ones until ``Collect`` will have ``{node_name: {task_id: List[CachingEvent]}}``.


Storage
--------

The caching feature is powered by two data storages:

- **Metadata store**: It contains information about past ``Driver`` executions (**code version**, **data version**, run id, etc.). From this metadata, Hamilton determines if a node needs to be executed or not. This metadata is generally lightweight.

- **Result store**: It's a key-value store that maps a **data version** to a **result**. It's completely unaware of nodes, executions, etc. and simply holds the **results**. The result store can significantly grow in size depending on your usage. By default, all results are pickled, but :ref:`other formats are possible <cache-result-format>`.


Configure storage
~~~~~~~~~~~~~~~~~~~

By default, the cache will be in a ``hamilton_cache/`` subdirectory, next to the current directory at executiont time. This path can be modified with the ``path`` parameter of ``Builder.with_cache()``. This will move the metadata store, result store, and result files should be stored.

This allows to same the share cache for a project or even globally.

.. code-block:: python

    from hamilton import driver

    dr = (
        driver.Builder()
        .with_modules(dataflow)
        .with_cache(path="~/.hamilton_cache")
        .build()
    )


If you want the metadata and result stores to be at different location, you can instantiate and pass them to ``.with_cache()``. Note that this will ignore the ``path`` parameter for this store.


.. code-block:: python

    from hamilton import driver
    from hamitlon.io.store import SQLiteMetadataStore, ShelveResultStore

    metadata_store = SQLiteMetadataStore(path="~/.hamilton_cache")
    result_store = ShelveResultStore(path="/path/to/my/project")

    dr = (
        driver.Builder()
        .with_modules(dataflow)
        .with_cache(
            metadata_store=metadata_store,
            result_store=result_store,
        )
        .build()
    )


Manually inspect storage
~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to directly interact with the metadata and result stores either by creating them or via ``Driver.cache``.


.. code-block:: python

    from hamitlon.io.store import SQLiteMetadataStore, ShelveResultStore

    metadata_store = SQLiteMetadataStore(path="~/.hamilton_cache")
    result_store = ShelveResultStore(path="/path/to/my/project")

    metadata_store.get(context_key=...)
    result_store.get(data_version=...)


.. code-block:: python

    from hamilton import driver
    import my_dataflow

    dr = (
        driver.Builder()
        .with_modules(dataflow)
        .with_cache()
        .build()
    )

    dr.cache.metadata_store.get(context_key=...)
    dr.cache.result_store.get(data_version=...)


A useful pattern is using the ``Driver.cache`` state or `structured logs <caching-structured-logs>` to retrieve a **data version** and query the **result store**.

.. code-block:: python

    from hamilton import driver
    from hamilton.lifecycle.caching import CachingEventType
    import my_dataflow

    dr = (
        driver.Builder()
        .with_modules(dataflow)
        .with_cache()
        .build()
    )

    dr.execute(["amount_per_country"])

    # via `cache.data_versions`; this points to the latest run
    data_version = dr.cache.data_versions["amount_per_country"]
    stored_result = dr.cache.result_store.get(data_version)

    # via structured logs; this allows to query any run
    run_id = ...
    for event in dr.cache.logs(level="debug")[run_id]:
        if (
            event.event_type == CachingEventType.SET_RESULT
            and event.node_name == "amount_per_country"
        ):
            data_version = event.value
            break

    stored_result = dr.cache.result_store(data_version)


Additional ``result_store`` and ``metadata_store`` backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The initial release of the caching feature included an ``sqlite``-backed metadata store and a ``shelve``-backed result store because both only depend on the standard library and work great for a single machine.

Support for ``AWS S3`` as a **result store** in on the roadmap. If you're interested in implementing a specific backend, please reach out via `Slack <https://join.slack.com/t/hamilton-opensource/shared_invite/zt-2niepkra8-DGKGf_tTYhXuJWBTXtIs4g>`_ since these API are currently unstable.


Code version
--------------

The code version of a node is determined via the ``HamiltonNode.version`` attribute which hashes the source code of a node, ignoring the docstring and comments. For example:

.. code-block:: python

    def _increment(x):
        return x + 1

    def foo():
        return _increment(13)

    # foo's code version: 129064d4496facc003686e0070967051ceb82c354508a58440910eb82af300db


Importantly, Hamilton will not version nested function calls. If you edit utility functions or upgrade Python libraries, the cache might incorrectly assume the code to be the same.

In the next snippet, we change ``_increment``, which should change the result of ``foo``. Because we get the same code version, the cache will incorrectly skip ``foo`` and load the value ``13 + 1`` instead of ``13 + 2``.

.. code-block:: python

    def _increment(x):
        return x + 2

    def foo():
        return _increment(13)

    # foo's code version: 129064d4496facc003686e0070967051ceb82c354508a58440910eb82af300db


In that case, you should force recompute by caching the caching behavior, or deleting the stored metadata or results.

Data version
-------------

Caching requires the ability to uniquely identify data (e.g., create a hash). By default, all Python primitive types (``int``, ``str``, ``dict``, etc.) are supported and more types can be added via extensions (e.g., ``pandas``). For types not explicitly supported, caching can still function by versioning the object's internal ``__dict__`` instead. However, this could be expensive to compute or less reliable than alternatives.

Recursion depth
~~~~~~~~~~~~~~~~

To version complex objects, we recursively hash its values. For example, versioning an object ``List[Dict[str, float]]`` involves hashing all keys and values of all dictionaries. Versioning complex objects with large ``__dict__`` state can become expensive.

In practice, we need to need a maximum recursion depth because there's a trade-off between the computational cost of hashing data and how accurately it uniquely identifies data (reduce hashing collisions). The max recursion depth can be set via the ``hamilton.io.fingerprinting`` module. By default, ``MAX_DEPTH=4``

.. code-block:: python

    from hamilton.io import fingerprinting

    fingerprinting.MAX_DEPTH = 3


Support additional types
~~~~~~~~~~~~~~~~~~~~~~~~~

Additional types can be supported by registering a hashing function via the module ``hamilton.io.fingerprinting``. It uses `@functools.singledispatch <https://docs.python.org/3/library/functools.html#functools.singledispatch>`_ to register the hashing function per Python type. The function must return a ``str``. The code snippets shows how to support polars ``DataFrame``:

.. code-block:: python

    import polars as pl
    from hamilton.io import fingerprinting

    # specify the type via the decorator
    @fingerprinting.hash_value.register(pl.DataFrame)
    def hash_polars_dataframe(obj, *args, **kwargs) -> str:
        """Convert a polars dataframe to a list of row hashes, then hash the list.
        We consider that row order matters.
        """
        # obj is of type `pl.DataFrame`
        hash_per_row = obj.hash_rows(seed=0)
        # fingerprinting.hash_value(...) will automatically hash primitive Python types
        return fingerprinting.hash_value(hash_per_row)


Roadmap
-----------

Caching is a significant Hamilton feature and there are plans to expand it. Here are some ideas and areas for development. Feel free comment on them or make other suggestions via Slack or GitHub!

- **async support**: Support caching with ``AsyncDriver``. This requires a significant amount of code, but the core logic shouldn't change much.

- **cache eviction**: Allow to set up a max storage (in size or number of items) or time-based policy to delete data from the metadata and result stores. This would help with managing the cache size.

- **more store backends**: The initial release includes backend supported by the Python standard library (SQLite metadata and file-based results). Could support more backends via `fsspec
<https://filesystem-spec.readthedocs.io/en/latest/?badge=latest>`_ (AWS, Azure, GCP, Databricks, etc.)

- **support more types**: Include specialized hashing functions for complex objects from popular libraries. This can be done through Hamilton extensions.
