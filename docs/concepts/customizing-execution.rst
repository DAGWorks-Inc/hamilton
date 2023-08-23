Customizing Execution
----------------------------------


The Driver
----------------------------------
The Hamilton Driver by default has the following behaviors:

#. It is single threaded, and runs on the machine you call execute from.
#. It is limited to the memory available on your machine.
#. ``execute()`` by default returns a pandas DataFrame.

To change these behaviors, we need to introduce two concepts:

#. A Result Builder -- this is how we tell Hamilton what kind of object we want to return when we call ``execute()``.
#. A Graph Adapters -- this is how we tell Hamilton where and how functions should be executed.

Result Builders
###############

In effect, this is a class with a static function, that takes a dictionary of computed results, and turns it into
something.

.. code-block:: python

    class ResultMixin(object):
        """Base class housing the static function.

        Why a static function? That's because certain frameworks can only pickle a static function, not an entire
        object.
        """
        @staticmethod
        @abc.abstractmethod
        def build_result(**outputs: typing.Dict[str, typing.Any]) -> typing.Any:
            """This function builds the result given the computed values."""
            pass

So we have a few implementations see :doc:`../../reference/result-builders/index` for the list.

To use it, it needs to be paired with a GraphAdapter - onto the next section!

Graph Adapters
##############

Graph Adapters `adapt` the Hamilton DAG, and change how it is executed. They all implement a single interface called
``base.HamiltonGraphAdapter``. They are called internally by Hamilton at the right points in time to make execution
work. The link with the Result Builders, is that GraphAdapters need to implement a ``build_result()`` function
themselves.

.. code-block:: python

    class HamiltonGraphAdapter(ResultMixin):
        """Any GraphAdapters should implement this interface to adapt the HamiltonGraph for that particular context.

        Note since it inherits ResultMixin -- HamiltonGraphAdapters need a `build_result` function too.
        """
        # four functions not shown

The default GraphAdapter is the ``base.SimplePythonDataFrameGraphAdapter`` which by default makes Hamilton try to build
a ``pandas.DataFrame`` when ``.execute()`` is called.

If you want to tell Hamilton to return something else, we suggest starting with the ``base.SimplePythonGraphAdapter``
and writing a simple class & function that implements the ``base.ResultMixin`` interface and passing that in.  See
:doc:`../reference/graph-adapters/index` and
:doc:`../reference/result-builders/index` for options.

Otherwise, let's quickly walk through some options on how to execute a Hamilton DAG.

Local Execution
***************

You have two options:

#. Do nothing -- and you'll get ``base.SimplePythonDataFrameGraphAdapter`` by default.
#.  Use ``base.SimplePythonGraphAdapter`` and pass in a subclass of ``base.ResultMixin`` (you can create your own), and then pass that to the constructor of the Driver.

    e.g.

.. code-block:: python

    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    dr = driver.Driver(..., adapter=adapter)

By passing in ``base.DictResult()`` we are telling Hamilton that the result of ``execute()`` should be a dictionary with
a map of ``output`` to computed result.

Note that the above is the most common method of executing Hamilton DAGs. You can also use `base.DefaultAdapter`
to get a `SimplePythonGraphAdapter` with a `DictResult`.

Dynamic DAGs/Parallel Execution
----------------------------------

Hamilton now has pluggable execution, which allows for the following:

1. Grouping of nodes into "tasks" (discrete execution unit between serialization boundaries)
2. Executing the tasks in parallel, using any executor of your choice

You can run this executor using the `Builder`, a utility class that allows you to build a driver piece by piece.
Note that you currently have to call `enable_dynamic_execution(allow_experimental_mode=True)`
which will toggle it to use the `V2` executor. Then, you can:

1. Add task executors to specify how to run the tasks
2. Add node gropuing strategies
3. Add modules to crawl for functions
4. Add a results builder to shape the results

Either constructing the driver, or using the builder and `not` calling `enable_dynamic_execution` will give you the standard executor.
We highly recommend you use the builder pattern -- while the constructor of the `Driver` will be fully
backwards compatible according to the rules of semantic versioning, we may change it in the future (for 2.0).

Note that the new executor is required to handle dynamic creation of nodes (E.G. using `Parallelizable[]` and `Collect[]`.

Let's look at an example of the driver:

.. code-block:: python

    from my_code import foo_module, bar_module

    from hamilton import driver
    from hamilton.execution import executors

    dr = (
        driver.Builder()
        .with_modules(foo_module)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({"config_key": "config_value"})
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
        .build()
    )

    dr.execute(["my_variable"], inputs={...}, overrides={...})

Note that we set a `remote` executor, and a local executor. While you can bypass this and instead set an `execution_manager`
in the builder call (see :doc:`../reference/drivers/Driver` for documentation on the `Builder`),this goes along with the default grouping strategy,
which is to place each node in its own group, except for
dynamically generated (`Parallelizable[]`) blocks, which are each made into one group, and executed locally.

Thus, when you write a DAG like this (a simple map-reduce pattern):

.. code-block:: python

    from hamilton.htypes import Parallelizable

    def url() -> Parallelizable[str]:
        for url_ in  _list_all_urls():
            yield url_

    def url_loaded(url: str) -> str:
        return _load(urls)

    def counts(url_loaded: str) -> str:
        return len(url_loaded.split(" "))

    def total_words(counts: Collect[int]) -> int:
        return sum(counts)

The block containing `counts` and `url_loaded` will get marked as one task, repeated for each URL in url_loaded,
and run on the remote executor (which in this case is the `ThreadPoolExecutor`).

Note that we currently have the following caveats:

1. No nested `Parallelizable[]`/`Collect[]` blocks -- we only allow one level of parallelization
2. Serialization for `Multiprocessing` is suboptimal -- we currently use the default `pickle` serializer, which breaks with certain cases. Ray, Dask, etc... all work well, and we plan to add support for joblib + cloudpickle serialization.
3. `Collect[]` input types are limited to one per function -- this is another caveat that we intend to get rid of, but for now you'll want to concat/put into one function before collecting.
