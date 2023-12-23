======================
Custom Result Builders
======================

How to build your own Result Builder

Before starting
---------------

We suggest reaching out on `slack <https://join.slack.com/t/hamilton-opensource/shared\_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg>`__ \
or via a github issue, if you have a use case for a custom result builder. Knowing about your use case and talking \
through it can help ensure we aren't duplicating effort, and we can help steer you in the right direction.

What you need to do
-------------------

You need to implement a class that implements a single function - see \
`github code <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/base.py#L18-L28>`__:

.. code-block:: python

    class ResultBuilder(object):
        """Base class housing the result builder"""
        @abc.abstractmethod
        def build_result(self, **outputs: typing.Dict[str, typing.Any]) -> typing.Any:
            """This function builds the result given the computed values."""
            pass

E.g.

.. code-block:: python

    import typing
    from hamilton import customization
    class MyCustomBuilder(customization.ResultBuilder):
         # add a constructor if you need to
         @staticmethod
         def build_result(**outputs: typing.Dict[str, typing.Any]) -> YOUR_RETURN_TYPE:
             """Custom function you fill in"""
             # your logic would go here
             return OBJECT_OF_YOUR_CHOOSING

How to use it
-------------

You would then have the option to pair that with a graph adapter that takes in a ResultMixin object. E.g. ``SimplePythonGraphAdapter``.
See :doc:`../customizing-execution/index` for which ones take in a custom ResultMixin object.

You can pass the result builder or a graph adapters to the ``driver.Builder(result_builder).with_adapters(...)``
function.
