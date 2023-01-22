===================
The Hamilton Driver
===================

Writing functions is great, but its meaningless if you have no way to execute them. We currently have a single Hamilton
Driver, that is responsible for the following:

#. Crawling your python modules to extract functions to turn into nodes in the DAG.
#. Running the DAG.
#. Assembling the results.
#. Enabling you to visualize the DAG.

It is provided as an easy way for the user to specify the data she wants without dealing with the complexities of DAGs,
function graphs, or nodes.

The basic structure of using the Hamilton Driver is:

.. code-block:: python

    from hamilton import driver
    from hamilton import base

    # 1. Setup config. See the Parameterizing the DAG section for usage
    config = {}

    # 2. we need to tell hamilton where to load function definitions from
    module_name = 'my_functions'
    module = importlib.import_module(module_name)  # or simply "import my_functions"

    # 3. Determine the return type -- default is a pandas.DataFrame.
    adapter = base.SimplePythonDataFrameGraphAdapter() # See GraphAdapter docs for more details.

    # These all feed into creating the driver & thus DAG.
    dr = driver.Driver(config, module, adapter=adapter)

    # lastly to get something, we need to call execute
    result = dr.execute(['desired_output1', 'desired_output2'])

    # pip install sf-hamilton[visualization] for this next line to work:
    dr.visualize_execution(['desired_output1', 'desired_output2'], './my_file.dot, {})

Note, currently, the stock Hamilton driver, is the API interface to use, to execute your Hamilton dataflows. Before
diving into how to call/use the driver more, let's cover DAG parameterization.

.. _parameterizing-the-dag:

Parameterizing the DAG
------------------------------

Static dataflows are only so useful. In the real world, we need to be able to configure both the shape of the DAG and
the inputs to the DAG as part of the Hamilton driver. The default Hamilton driver comes with three input types that you
can control. They both take the form ``Dict[str, Any]``, i.e. a dictionary of string keys that maps to any object type.

#. **config** The config is a dictionary of strings to values. This is passed into the constructor of the Hamilton driver, as it is required to create the DAG. It `also` get's passed into the DAG at runtime, so you have access to parameter values. See :doc:`decorators` for how the config can be used.
#. **inputs** The `runtime inputs` to the DAG. These have to be mutually disjoint from the config -- overrides don't make sense here, as the DAG has been constructed assuming fixed configs.
#. **overrides** Values to override nodes in a DAG. During execution, nothing upstream of these are computed.

Let's go through some examples that show you how to write a Hamilton function that allows it to be conditionally used
depending on configuration.

**You have a DAG for region and business line**, where the rolling average for marketing spend is computed differently
(see :doc:`../less-than-15-minutes-to-mastery/index` for the motivating example). In this case, you'll define the DAG as follows:

.. code-block:: python

    @config.when(business_line='CA')
    def avg_rolling_spend__CA(spend: pd.Series) -> pd.Series:
        """Rolling average of spend in the canada region."""
        return spend.rolling(3).mean()

    @config.when(business_line='US')
    def avg_rolling_spend__US(spend: pd.Series) -> pd.Series:
        """Rolling average of spend in the US region."""
        return spend.rolling(2).mean()

When the graph is compiled, the implementation of ``avg_rolling_spend`` varies based off of the configuration value.
You would construct the driver with ``config={'region' : 'US'}``, to get the desired behavior.

**You want to pass in the region/business line to change the behavior or a transform.** Say you have a big dataframe of
marketing spend with columns representing the region, and also want to filter it out for the individual region. You
would define the transform function as follows.

.. code-block:: python

    def avg_rolling_spend(spend_by_country: pd.DataFrame, region: str) -> pd.Series:
        """Rolling average of spend in the specified region."""
        return spend_by_country[spend_by_country.region==region].spend

You would execute the driver with ``input={'region' : 'US'}``, to get the desired behavior. You could `also` construct
the DAG with ``config={'region' : 'US'}``.

**You want to override the value of a transform**. In this case, you can just pass this into the execute function of the
driver as overrides. E.G.:

.. code-block:: python

    df = dr.execute(
        ['acquisition_cost'],
        overrides={'spend' : pd.Series(
            [40, 80, 100, 400, 800, 1000], # what if we increased the marketing spend?
            index=pd.date_range("2022-01-01", periods=6, freq="w"))})

Building a Result & Graph Adapters
----------------------------------

The stock Hamilton Driver by default has the following behaviors:

#. It is single threaded, and runs on the machine you call execute from.
#. It is limited to the memory available on your machine.
#. ``execute()`` by default returns a pandas DataFrame.

To change these behaviors, we need to introduce two concepts:

#. A Result Builder -- this is how we tell Hamilton, what kind of object we want to return when we call ``execute()``.
#. A Graph Adapters -- this is how we tell Hamilton, where and how functions should be executed.

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

So we have a few implementations see :doc:`../reference/api-reference/available-result-builders` for the list.

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
:doc:`../reference/api-reference/available-graph-adapters` and
:doc:`../reference/api-reference/available-result-builders` for options.

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

Scaling Hamilton: Multi-core & Distributed Execution
****************************************************

This functionality is currently in an "experimental" state. We think the code is solid, but it hasn't been used in a
production environment for long. Thus the API to these GraphAdapters might change.

See the `experimental <https://github.com/stitchfix/hamilton/tree/main/hamilton/experimental>`_ package for the current
implementations. We encourage you to give them a spin and provide us with feedback. See
:doc:`../reference/api-reference/available-graph-adapters` for more details.

Calling Execute()
#################

There are two ways to use ``execute()``:

#. Call it once -- you only request the outputs required. E.g. ``dr.execute(['desired_output1', 'desired_output2'])``
#. Call it in succession by providing it specific inputs, in addition to the outputs required. E.g. ``dr.execute(['desired_output1', 'desired_output2'], inputs={...})``

We recommend using option (1) where possible. Option (2) only makes sense if you want to reuse the dataflow created for
different data sets, or to chunk over large data or iterate over objects, e.g. images or text.

Visualizing Execution
#####################

Hamilton enables you to quickly and easily visualize your entire DAG, as well as the specific execution path to compute
an output. Underneath we default to use [graphviz](https://graphviz.org/) for visualization.

Visualize just execution required to create outputs
***************************************************

.. code-block:: python

    dr.visualize_execution(['desired_output1', 'desired_output2'], './my_file.dot', render_args)

In addition to specifying the outputs you desire, you need to provide a path to save the created dot file and image, and
then provide some  arguments for rendering -- at minimum, pass in an empty dictionary.

Visualize the entire DAG constructed
************************************

.. code-block:: python

    dr.display_all_functions('./my_file.dot', render_args)

You need to provide a path to save the created dot file and image, and then provide some optional arguments for
rendering.

Should I define my own Driver?
------------------------------

The APIs that the Hamilton Driver is built on, are considered internal. So it is possible for you to define your own
driver in place of the stock Hamilton driver, we suggest the following path if you don't like how the current Hamilton
Driver interface is designed:

`Write a "Wrapper" class that delegates to the Hamilton Driver.`

i.e.

.. code-block:: python

    from hamilton import driver

    class MyCustomDriver(object):
        def __init__(self, constructor_arg, ...):
           self.constructor_arg = constructor_arg
           ...
        # some internal functions specific to your context
        # ...

        def my_execute_function(self, arg1, arg2, ...):
            """What actually calls the Hamilton"""
            dr = driver.Driver(self.constructor_arg, ...)
            df = dr.execute(self.outputs)
            return self.augmetn(df)

That way, you can create the right API constructs to invoke Hamilton in your context, and then delegate to the stock
Hamilton Driver. By doing so, it will ensure that your code continues to work, since we intend to honor the Hamilton
Driver APIs with backwards compatibility as much as possible.
