==========
Extensions
==========

Hamilton structurally provides the foundation for several extensions: parallel computation, distributed computation,
creating optimized machine code, controlling return object types.

Scaling Hamilton: Parallel & Distributed Computation
----------------------------------------------------

*Note that the following is still available, but we have a far more sophisticated capability for carrying this out!*

See :doc:`concepts/customizing-execution` for more information.

Hamilton by default runs in a single process and single threaded manner.

Wouldn't it be great if it could execute computation in parallel if it could? Or, if you could scale to data sets that
can't all fit in memory? What if you `didn't have to change` your Hamilton functions?

Well, with the simple change of some driver script code, you can very easily scale your Hamilton dataflows, especially
if you write Pandas code!

All that's needed is to:

#. Import system specific code to setup a client/cluster/etc for that distributed/scalable system.
#. Import a `GraphAdapter <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/base.py#L91>`_ that implements \
   using that distributed/scalable system. See :doc:`reference/graph-adapters/index` for what is \
   available.
#. You may need to provide a specific module that knows how to load data into the scalable system.
#. Pass the modules, and graph adapter to the Hamilton Driver.
#. Proceed as you would normally.

.. code-block:: python

    from hamilton import driver
    from hamilton.plugins import h_dask  # import the correct module

    from dask.distributed import Client  # import the distributed system of choice
    client = Client(...)  # instantiate the specific client

    dag_config = {...}
    bl_module = importlib.import_module('my_functions') # business logic functions
    loader_module = importlib.import_module('data_loader') # functions to load data
    adapter = h_dask.DaskGraphAdapter(client) # create the right GraphAdapter

    dr = driver.Driver(dag_config, bl_module, loader_module, adapter=adapter)
    output_columns = ['year','week',...,'spend_shift_3weeks_per_signup','special_feature']
    df = dr.execute(output_columns) # only walk DAG for what is needed

See :doc:`reference/graph-adapters/index` and :doc:`reference/api-extensions/custom-graph-adapters`
for options.

Ray
===

`Ray <https://ray.io>`_ is a system to scale python workloads. Hamilton makes it very easy for you to use Ray.

See `Scaling Hamilton on Ray <https://github.com/dagworks-inc/hamilton/tree/main/examples/ry>`_
for an example of using Ray.

Single Machine:
***************

Ray is a very easy way to enable multi-processing on a single machine. This enables you to easily make use of multiple
CPU cores.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine.

Distributed Computation:
************************

If you have a Ray cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel
compute, and the potential to scale to large data set sizes, however, you'll be limited to the size of a single machine
in terms of the amount of data it can process.

Dask
====

`Ray <https://ray.io>`_ is a system to scale python workloads. Hamilton makes it very easy for you to use Ray.

See `Scaling Hamilton on Dask <https://github.com/dagworks-inc/hamilton/tree/main/examples/dask>`_
for an example of using Dask to scale Hamilton computation.

Single Machine:
***************

`Dask <https://dask.org>`_ is a very easy way to enable multi-processing on a single machine. This enables you to
easily make use of multiple CPU cores.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine
Distributed Computation:
************************

If you have a Dask cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel
compute, and the ability to scale to petabyte scale data set sizes.

Koalas on Spark, a.k.a. Pandas API on Spark
===========================================

`Spark <https://spark.apache.org/>`_ is a scalable data processing framework. `Koalas <https://koalas.readthedocs.io/en/latest>`_
was the project code name to implement the \
`Pandas API on top of Spark <https://spark.apache.org/docs/latest/api/python/user\_guide/pandas\_on\_spark/index.html>`__.
Hamilton makes it very easy for you to use Koalas on Spark.

See `Scaling Hamilton on Koalas <https://github.com/dagworks-inc/hamilton/tree/main/examples/spark>`_
for an example of using Koalas on Spark to scale Hamilton computation.

Single Machine:
***************

You will very likely not want to use Spark on a single machine. It does enable multi-processing, but is likely inferior
to Ray or Dask.

What this doesn't help with is data scale, as you're still limited to what fits in memory on your machine.

Distributed Computation:
************************

If you have a Spark cluster setup, then you can farm out Hamilton computation to it. This enables lots of parallel
compute, and the ability to scale to petabyte scale data set sizes.

Customizing what Hamilton Returns
---------------------------------

Hamilton grew up with a Pandas Dataframe assumption. However, as of the ``1.3.0`` release, **Hamilton is a general
purpose dataflow framework.**

This means, that the result of ``execute()`` can be any python object type!

How do you change the type of the object returned?
==================================================

You need to implement a `ResultMixin <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/base.py#L18>`__ if \
there isn't one already defined for what you want to do. Then you need to provide that to a \
`GraphAdapter <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/base.py#L91>`__, similar to what was \
presented above.

See :doc:`reference/result-builders/index` for what is provided with Hamilton, or \
:doc:`reference/api-extensions/custom-result-builders` for how to build your own.

.. code-block:: python

    from dask.distributed import Client
    from hamilton import driver
    from hamilton import base

    adapter = base.DefaultAdapter# or your custom class

    dr = driver.Driver(dag_config, bl_module, loader_module, adapter=adapter)

    output_columns = ['year','week',...,'spend_shift_3weeks_per_signup','special_feature']
    # creates a dict of {col -> function result}

    result_dict = dr.execute(output_columns)
