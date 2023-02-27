========================
Graph Adapters
========================

Graph adapters control how functions are executed as the graph is walked.

.. autoclass:: hamilton.base.HamiltonGraphAdapter
   :members:

.. autoclass:: hamilton.base.SimplePythonDataFrameGraphAdapter
   :members:

.. autoclass:: hamilton.base.SimplePythonGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:


Experimental Graph Adapters
---------------------------

The following are considered experimental; there is a possibility of their API changing. That said, the code is stable,
and you should feel comfortable giving the code for a spin - let us know how it goes, and what the rough edges are if
you find any. We'd love feedback if you are using these to know how to improve them or graduate them.


Ray
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: hamilton.experimental.h_ray.RayGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:

.. autoclass:: hamilton.experimental.h_ray.RayWorkflowGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:


Dask
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: hamilton.experimental.h_dask.DaskGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:


Pandas on Spark (Koalas)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
..  autoclass:: hamilton.experimental.h_spark.SparkKoalasGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:


Async Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: hamilton.experimental.h_async.AsyncGraphAdapter
   :special-members: __init__
   :members:
   :inherited-members:
