=======================
check_output*
=======================
The ``@check_output`` decorator enables you to add simple data quality checks to your code.

For example:

.. code-block:: python

    import pandas as pd
    import numpy as np
    from hamilton.function_modifiers import check_output

    @check_output(
        data_type=np.int64,
        data_in_range=(0,100),
    )
    def some_int_data_between_0_and_100() -> pd.Series:
        pass

The check\_output validator takes in arguments that each correspond to one of the default validators. These arguments
tell it to add the default validator to the list. The above thus creates two validators, one that checks the datatype
of the series, and one that checks whether the data is in a certain range.

Note that you can also specify custom decorators using the ``@check_output_custom`` decorator.

See `data_quality <https://github.com/dagworks-inc/hamilton/blob/main/data\_quality.md>`_ for more information on
available validators and how to build custom ones.

----

**Reference Documentation**

.. autoclass:: hamilton.function_modifiers.check_output
   :special-members: __init__

.. autoclass:: hamilton.function_modifiers.check_output_custom
   :special-members: __init__
