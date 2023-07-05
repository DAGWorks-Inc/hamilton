Customizing Execution
----------------------------------

The stock Hamilton Driver by default has the following behaviors:

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

Scaling Hamilton: Multi-core & Distributed Execution
****************************************************

This functionality is currently in an "experimental" state. We think the code is solid, but it hasn't been used in a
production environment for long. Thus the API to these GraphAdapters might change.

See the `experimental <https://github.com/dagworks-inc/hamilton/tree/main/hamilton/experimental>`_ package for the current
implementations. We encourage you to give them a spin and provide us with feedback. See
:doc:`../reference/graph-adapters/index` for more details.
