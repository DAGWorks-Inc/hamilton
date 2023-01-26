========================
Available Graph Adapters
========================

Here we list available graph adapters

Use ``from hamilton import base`` to use these Graph Adapters:

.. list-table::
   :header-rows: 1

   * - Name
     - What it does
     - When you'd use it
   * - `base.SimplePythonDataFrameGraphAdapter <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L134>`_
     - This executes the Hamilton dataflow locally on a machine in a single threaded, single process fashion. It assumes a pandas dataframe as a result.
     - This is the default GraphAdapter that Hamilton uses. Use this when you want to execute on a single machine, without parallelization, and you want a pandas dataframe as output.
   * - `base.SimplePythonGraphAdapter <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L149>`_
     - This executes the Hamilton dataflow locally on a machine in a single threaded, single process fashion. It allows you to specify a ResultBuilder to control the return type of what ``execute()`` returns.
     - This is the default GraphAdapter that Hamilton uses. Use this when you want to execute on a single machine, without parallelization, and you want to control the return type of the object that ``execute()`` returns.

Experimental Graph Adapters
---------------------------

The following are considered experimental; there is a possibility of their API changing. That said, the code is stable,
and you should feel comfortable giving the code for a spin - let us know how it goes, and what the rough edges are if
you find any.

Use ``from hamilton.experimental import h_[NAME]`` to use these Graph Adapters:

.. list-table::
   :header-rows: 1

   * - Name
     - What it does
     - When you'd use it
   * - `h_dask.DaskGraphAdapter <https://github.com/stitchfix/hamilton/blob/main/hamilton/experimental/h_dask.py#L21>`_
     - | This walks the graph and translates it to run onto `Dask <https://dask.org/>`_.
       | You have the ability to pass in a ResultMixin object to the constructor to control the return type that gets produce by running on Dask.
     - Use this if you want to utilize multiple cores on a single machine, or you want to scale to large data set sizes with a Dask cluster that you can connect to.
   * - `h_ray.RayGraphAdapter <https://github.com/stitchfix/hamilton/blob/main/hamilton/experimental/h_ray.py#L12>`_
     - | This walks the graph and translates it to run onto `Ray <https://ray.io/>`_.
       | You have the ability to pass in a ResultMixin object to the constructor to control the return type that gets produce by running on Ray.
     - Use this if you want to utilize multiple cores on a single machine, or you want to scale to larger data set sizes with a Ray cluster that you can connect to. Note: you are still constrained by machine memory size with Ray; you can't just scale to any dataset size.
   * - `h_spark.SparkKoalasGraphAdapter <https://github.com/stitchfix/hamilton/blob/main/hamilton/experimental/h_spark.py#L25>`_
     - | This walks the graph and translates it to run onto `Apache Spark <https://spark.apache.org/">`_ using the `Pandas API on Spark <https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html>`_ (aka `Koalas <https://koalas.readthedocs.io/en/latest>`_).
       | You only have the ability to return either a Koalas Dataframe or a Pandas Dataframe. To do that you either use the stock `base.PandasDataFrameResult <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L39>`_ ResultMixin, or you use the `h_spark.KoalasDataframeResult <https://github.com/stitchfix/hamilton/blob/main/hamilton/experimental/h_spark.py#L16>`_.
     - | You'd generally use this if you have an existing spark cluster running in your workplace, and you want to scale to very large data set sizes.
       | Note this GraphAdapter has only been tested to work on Spark 3.2+ when Koalas became part of the standard Spark library.
