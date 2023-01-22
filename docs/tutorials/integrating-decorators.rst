======================
Integrating Decorators
======================

This follows up on :doc:`../less-than-15-minutes-to-mastery/index`.

Hamilton relies on `python decorators <https://towardsdatascience.com/the-simplest-tutorial-for-python-decorator-dadbf8f20b0f>`_
to enable easy code reuse. Taking the previous example, let's say that we cared about the running average spend per
signup with both a 2 and a 3 week lookback. Rather than writing a bunch of functions with almost exactly the same
definitions, we can parametrize! The following uses two decorator to `curry <https://en.wikipedia.org/wiki/Currying>`_
your nodes into multiple functions.

.. code-block:: python

    import pandas as pd

    from hamilton import function_modifiers
    from hamilton.function_modifiers import value, source


    @function_modifiers.parameterize(
        avg_2wk_spend={'rolling_lookback' : value(2)},
        avg_3wk_spend={'rolling_lookback' : value(3)}
    )
    def avg_nwk_spend(spend: pd.Series, rolling_lookback: int) -> pd.Series:
        """Average marketing spend looking back {rolling_lookback} weeks."""
        return spend.rolling(rolling_lookback).mean()


    @function_modifiers.parameterize(
        acquisition_cost_2wk={'spend' : source('avg_2wk_spend')},
        acquisition_cost_3wk={'spend' : source('avg_3wk_spend')}
    )
    def acquisition_cost(spend: pd.Series, signups: pd.Series) -> pd.Series:
        """The cost per signup in relation to {spend}."""
        return spend / signups

In this case we have two separate parameterizations:

#. Parameterizing the value (currying the function) for lookback
#. Parameterizing the source of the variable spend in acquisition\_cost

All we have to do is modify our driver to run the right module and ask for the right outputs, and we're good to go!

.. code-block:: python

    import logging
    import sys

    import pandas as pd

    import with_decorators  # we import the module here!
    from hamilton import driver

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)

    if __name__ == '__main__':
        # Instantiate a common spine for your pipeline
        index = pd.date_range("2022-01-01", periods=6, freq="w")
        initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
            # Note: these do not have to be all series, they could be scalar inputs.
            'signups': pd.Series([1, 10, 50, 100, 200, 400], index=index),
            'spend': pd.Series([10, 10, 20, 40, 40, 50], index=index),
        }
        # we need to tell hamilton where to load function definitions from
        dr = driver.Driver(initial_columns, with_decorators)  # can pass in multiple modules
        # we need to specify what we want in the final dataframe.
        output_columns = [
            'spend',
            'signups',
            'acquisition_cost_2wk',
            'acquisition_cost_3wk',
        ]
        # let's create the dataframe!
        df = dr.execute(output_columns)
        # `pip install sf-hamilton[visualization]` earlier you can also do
        # dr.visualize_execution(output_columns,'./my_dag.dot', {})
        print(df)

Running the driver now gives you the following:

.. code-block:: bash

       spend  signups  acquisition_cost_2wk  acquisition_cost_3wk
    0     10        1                   NaN                   NaN
    1     10       10                1.0000                   NaN
    2     20       50                0.3000              0.266667
    3     40      100                0.3000              0.233333
    4     40      200                0.2000              0.166667
    5     50      400                0.1125              0.108333
