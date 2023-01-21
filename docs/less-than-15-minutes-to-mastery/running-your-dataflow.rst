=====================
Running Your Dataflow
=====================

To actually run the dataflow, we'll need to write :doc:`../overview-of-concepts/the-hamilton-driver`. Create
``my_script.py`` with the following contents:

.. code-block:: python

    import logging
    import sys

    import pandas as pd

    import my_functions  # we import the module here!
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
        dr = driver.Driver(initial_columns, my_functions)  # can pass in multiple modules
        # we need to specify what we want in the final dataframe.
        output_columns = [
            'spend',
            'signups',
            'avg_3wk_spend',
            'acquisition_cost',
        ]
        # let's create the dataframe!
        df = dr.execute(output_columns)
        # `pip install sf-hamilton[visualization]` earlier you can also do
        # dr.visualize_execution(output_columns,'./my_dag.dot', {})
        print(df)

Run the script with the following command:

``python my_script.py``

And you should see the following output:

.. code-block:: bash

                spend  signups  avg_3wk_spend  acquisition_cost
    2022-01-02     10        1            NaN            10.000
    2022-01-09     10       10            NaN             1.000
    2022-01-16     20       50      13.333333             0.400
    2022-01-23     40      100      23.333333             0.400
    2022-01-30     40      200      33.333333             0.200
    2022-02-06     50      400      43.333333             0.125

Not only is your spend to signup ratio decreasing exponentially (your product is going viral!), but you've also
successfully ran your first Hamilton Dataflow. Kudos!

See, wasn't that quick and easy?
