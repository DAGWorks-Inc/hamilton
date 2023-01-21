===========================
Writing Your First Dataflow
===========================

We're jumping in head-first. If you want to start with an overview, skip ahead to
:doc:`../overview-of-concepts/index`.

.. note::

    You can follow along in the `examples directory <https://github.com/stitchfix/hamilton/tree/main/examples/hello\_world>`_
    of the `hamilton repo <https://github.com/stitchfix/hamilton/>`_. We highly recommend forking the repo and playing
    around with the code to get comfortable.

Writing some Transforms
-----------------------

Create a file ``my_functions.py`` and add the following two functions:

.. code-block:: python

    import pandas as pd

    def avg_3wk_spend(spend: pd.Series) -> pd.Series:
        """Rolling 3 week average spend."""
        return spend.rolling(3).mean()

    def acquisition_cost(avg_3wk_spend: pd.Series, signups: pd.Series) -> pd.Series:
        """The cost per signup in relation to a rolling average of spend."""
        return avg_3wk_spend / signups

An astute observer might ask the following questions:

#. **Why do the parameter names clash with the function names?** This is core to how hamilton works. It utilizes dependency injection to create a DAG of computation. Parameter names tell the framework where your function gets its data.
#. **OK, if the parameter names determine the source of the data, why have we not defined defined `spend` or `signups` as functions?** This is OK, as we will provide this data as an input when we actually want to materialize our functions. The DAG doesn't have to be complete when it is compiled.
#. **Why is there no main line to call these functions?** Good observation. In fact, we never will call them (directly)! This is one of the core principles of Hamilton. You write individual transforms and the rest is handled by the framework. More on that next.
#. **The functions all output pandas series. What if I don't want to use series?** You don't have to! Hamilton is not opinionated on the data type you use. The following are all perfectly valid as well (and we support dask/spark/ray/other distributed frameworks).

Let's add a few more functions to our ``my_functions.py`` file:

.. code-block:: python

    def spend_mean(spend: pd.Series) -> float:
        """Shows function creating a scalar. In this case it computes the mean of the entire column."""
        return spend.mean()

    def spend_zero_mean(spend: pd.Series, spend_mean: float) -> pd.Series:
        """Shows function that takes a scalar. In this case to zero mean spend."""
        return spend - spend_mean

    def spend_std_dev(spend: pd.Series) -> float:
        """Function that computes the standard deviation of the spend column."""
        return spend.std()

    def spend_zero_mean_unit_variance(spend_zero_mean: pd.Series, spend_std_dev: float) -> pd.Series:
        """Function showing one way to make spend have zero mean and unit variance."""
        return spend_zero_mean / spend_std_dev

Let's give these functions a spin!
