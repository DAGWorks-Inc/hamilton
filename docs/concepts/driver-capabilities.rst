=====================
Running Your Code
=====================

Writing functions is great, but its meaningless if you have no way to execute them. We use "drivers" to execute the code.
We currently have a single Hamilton Driver that is responsible for the following:

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

Note that the stock Hamilton driver is the API interface to use to execute your Hamilton dataflows. Before
diving into how to call/use the driver more, let's cover DAG parameterization.

.. _parameterizing-the-dag:

Parameterizing the DAG
------------------------------

Static dataflows are only so useful. In the real world, we need to be able to configure both the shape of the DAG and
the inputs to the DAG as part of the Hamilton driver. The default Hamilton driver comes with three input types that you
can control. They both take the form ``Dict[str, Any]``, i.e. a dictionary of string keys that maps to any object type.

#. **config** The config is a dictionary of strings to values. This is passed into the constructor of the Hamilton driver, as it is required to create the DAG. It `also` gets passed into the DAG at runtime, so you have access to parameter values. See :doc:`decorators-overview`, as well as the examples below, for how the config can be used.

#. **inputs** The `runtime inputs` to the DAG. These have to be mutually disjoint from the config -- overriding the config does not make sense here, as the DAG has been constructed assuming fixed configs.

#. **overrides** Values to override nodes in a DAG. During execution, nothing upstream of these are computed.

Let's go through some examples that show you how to write a Hamilton function that allows it to be conditionally used
depending on configuration.

**You have a DAG for region and business line**, where the rolling average for marketing spend is computed differently
(see :doc:`../getting-started/index` for the motivating example). In this case, you'll define the DAG as follows:

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


Calling Execute()
#################

There are two ways to use ``execute()``:

#. Call it once -- you only request the outputs required. E.g. ``dr.execute(['desired_output1', 'desired_output2'])``
#. Call it in succession by providing it specific inputs, in addition to the outputs required. E.g. ``dr.execute(['desired_output1', 'desired_output2'], inputs={...})``

We recommend using option (1) where possible. Option (2) only makes sense if you want to reuse the dataflow created for
different data sets, or to chunk over large data or iterate over objects, e.g. images or text.

Materializing DAG Nodes
#######################

The driver comes with the ability to materialize DAG nodes -- this adds side-effects into the DAG to save the data it produces to various places.
These are fully customizable, and utilize the same set of constructs as :doc:`/reference/decorators/save_to/`.

It can be used to save data to a file, external data store, or a database -- its a flexible construct that comes with a few built-in options,
but is highly pluggable.

In the following case, we are joining `foo` and `bar` into a dataframe, then saving it to a CSV file:

.. code-block:: python

     from hamilton import driver, base
     from hamilton.io.materialize import to
     dr = driver.Driver(my_module, {})
     # foo, bar are pd.Series
     metadata, result = dr.materialize(
         to.csv(
             path="./output.csv",
             id="foo_bar_csv",
             dependencies=["foo", "bar"],
             combine=base.PandasDataFrameResult()
         ),
         additional_vars=["foo", "bar"]
     )

For more information, see `materialize` in the :doc:`driver </reference/drivers/Driver>`.

Visualizing Execution
#####################

Hamilton enables you to quickly and easily visualize your entire DAG, as well as the specific execution path to compute
an output. Underneath we default to use `graphviz <https://graphviz.org/>`_ for visualization.

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
