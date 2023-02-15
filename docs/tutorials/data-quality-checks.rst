===================
Data Quality Checks
===================

Hamilton now comes with data quality validations included! This is implemented through a simple decorator:
``check_output``.

Adding DQ to Your DataFlow
--------------------------

Let's take a look at ye old marketing spend example...

.. code-block:: python

    import pandas as pd

    def avg_3wk_spend(spend: pd.Series) -> pd.Series:
        """Rolling 3 week average spend."""
        return spend.rolling(3, min_periods=1).mean()

    def acquisition_cost(avg_3wk_spend: pd.Series, signups: pd.Series) -> pd.Series:
        """The cost per signup in relation to a rolling average of spend."""
        return avg_3wk_spend / signups

Now, let's say we want to assure a few things about ``acquisition_cost``...

#. That it consists of ``float`` (should be obvious from the code, but we want to be sure)
#. That it is greater than ``0`` (highly unlikely that a customer pays you for advertising)
#. That it is less than ``$1000`` (anything this high likely means a data issue)

Furthermore, let's say we want the pipeline to log a warning (as opposed to just failing out) if any of the above
conditions aren't met.

This is easy with the ``check_output`` decorator!

.. code-block:: python

    import pandas as pd
    import numpy as np
    from hamilton import function_modifiers

    def avg_3wk_spend(spend: pd.Series) -> pd.Series:
        """Rolling 3 week average spend."""
        return spend.rolling(3, min_periods=1).mean()

    @function_modifiers.check_output(
        range=(0,1000),
        data_type=np.float64,
        importance="warn"
    )
    def acquisition_cost(avg_3wk_spend: pd.Series, signups: pd.Series) -> pd.Series:
        """The cost per signup in relation to a rolling average of spend."""
        return avg_3wk_spend / signups

It takes in a series of arguments -- you can discover these by exploring the code (more documentation of specific
arguments to follow): `DefaultValidation <https://github.com/stitchfix/hamilton/blob/main/hamilton/data\_quality/default\_validators.py/>`_
You can also utilize ``pandera`` to the same effect, simply by providing a ``schema`` argument:

.. code-block:: python

    import pandas as pd
    import numpy as np
    from hamilton import function_modifiers

    def avg_3wk_spend(spend: pd.Series) -> pd.Series:
        """Rolling 3 week average spend."""
        return spend.rolling(3, min_periods=1).mean()

    @function_modifiers.check_output(
        schema=pa.SeriesSchema(
            pa.Column(float),
            pa.Check.in_range(0,1000)),
        importance="warn"
    )
    def acquisition_cost(avg_3wk_spend: pd.Series, signups: pd.Series) -> pd.Series:
        """The cost per signup in relation to a rolling average of spend."""
        return avg_3wk_spend / signups pyt

Finally, if you want to implement your own checks, you can explore the
`DataValidator <https://github.com/stitchfix/hamilton/blob/90afd3a08df15794f95f9741510923d089a6946a/hamilton/data\_quality/base.py#L26>`_
class and the ``check_output_custom`` decorator.

Running your Pipelines and Examining the Results
------------------------------------------------

Let's run the pipeline above to get the result of ``acquisition_cost``, injecting some bad data along the way...

.. code-block:: python

    import importlib
    import logging
    import sys

    import pandas as pd
    from hamilton import driver

    logging.basicConfig(stream=sys.stdout)
    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        # Note: these values don't have to be all series, they could be a scalar.
        'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': pd.Series([10, 10, 20, 40, 40, 50]),
    }
    # we need to tell hamilton where to load function definitions from
    module_name = 'my_functions'
    module = importlib.import_module(module_name)
    dr = driver.Driver(initial_columns, module)
    df = dr.execute(['acquisition_cost'])
    print(df.to_string())

When we run this, we get the following:

.. code-block:: bash

    python my_script.py
    WARNING:hamilton.data_quality.base:[acquisition_cost:range_validator]
    validator failed. Message was: Series contains 5 values in range (0,1000),
    and 1 outside.. Diagnostic information is:
    {'range': (0, 1000), 'in_range': 5, 'out_range': 1, 'data_size': 6}.

       acquisition_cost
    0         10.000000
    1       5000.500000
    2        666.866667
    3        333.533333
    4          0.166667
    5          0.483333

Note that it completed successfully, yet printed out the warning. When we set it to fail, we get an error (as
expected). Let's go back to look at the results, and see what we can learn...

If we modify our script to return the data quality results as well, we can capture the results for later use!

.. code-block:: python

    import importlib
    import logging
    import sys
    import dataclasses

    import pprint
    import pandas as pd
    from hamilton import base
    from hamilton import driver

    logging.basicConfig(stream=sys.stdout)
    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        # Note: these values don't have to be all series, they could be a scalar.
        'signups': pd.Series([1, 10, 50, 100, 200, 400]),
        'spend': pd.Series([10, 100000, 20, 40, 40, 500]),
    }
    # we need to tell hamilton where to load function definitions from
    module_name = 'my_functions'
    module = importlib.import_module(module_name)
    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    dr = driver.Driver(initial_columns, module, adapter=adapter)
    all_validator_variables = [
        var.name for var in dr.list_available_variables() if
        var.tags.get('hamilton.data_quality.contains_dq_results')]
    data = dr.execute(['acquisition_cost'] + all_validator_variables)
    pprint.pprint(dataclasses.asdict(data['acquisition_cost_range_validator']))
    pprint.pprint(dataclasses.asdict(data['acquisition_cost_data_type_validator']))

Note there's some magic above -- we're working on improving querying and reporting. As always, if you have ideas for the
API, let us know. Running the above yields the following:

.. code-block:: bash

    WARNING:hamilton.data_quality.base:[acquisition_cost:range_validator] validator failed. Message was: Series contains 5 values in range (0,1000), and 1 outside.. Diagnostic information is: {'range': (0, 1000), 'in_range': 5, 'out_range': 1, 'data_size': 6}.
    {'diagnostics': {'data_size': 6,
                     'in_range': 5,
                     'out_range': 1,
                     'range': (0, 1000)},
     'message': 'Series contains 5 values in range (0,1000), and 1 outside.',
     'passes': False}
    {'diagnostics': {'actual_dtype': dtype('float64'),
                     'required_dtype': <class 'numpy.float64'>},
     'message': "Requires subclass of datatype: <class 'numpy.float64'>. Got "
                'datatype: float64. This is a match.',
     'passes': True}

Knowing that data quality produces a series of nodes make this very powerful -- you can query and grab the results,
enabling you to programmatically react to them down the line.

This concludes our brief lessons on managing data quality in Hamilton -- we hope you spin it up and give it a try.

In the mean time, we have more detailed documentation here -
`data_quality.md <https://github.com/stitchfix/hamilton/blob/main/data\_quality.md/>`_
and some more examples (including distributed systems support) -
`data_quality examples <https://github.com/stitchfix/hamilton/tree/main/examples/data\_quality/>`_.
