import logging
import sys
from typing import Dict

import polars as pl

from hamilton import base, driver

logging.basicConfig(stream=sys.stdout)


class PolarsDataFrameResult(base.ResultMixin):
    """We need to create a result builder for our use case. Hamilton doesn't have a standard one
    to use just yet. If you think it should -- let's chat and figure out a way to make it happen!
    """

    def build_result(self, **outputs: Dict[str, pl.Series]) -> pl.DataFrame:
        """This is the method that Hamilton will call to build the final result. It will pass in the results
        of the requested outputs that you passed in to the execute() method.
        :param outputs: The results of the requested outputs.
        :return: a polars DataFrame.
        """
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, pl.DataFrame):  # it's a dataframe
                return value
            elif not isinstance(value, pl.Series):  # it's a single scalar
                return pl.DataFrame([outputs])
        # TODO: check for length of outputs and determine what should
        # happen for mixed outputs that include scalars for example.
        return pl.DataFrame(outputs)


adapter = base.SimplePythonGraphAdapter(result_builder=PolarsDataFrameResult())
config = {
    "base_df_location": "dummy_value",
}
import my_functions  # where our functions are defined

dr = driver.Driver(config, my_functions, adapter=adapter)
# note -- we cannot request scalar outputs like we could do with Pandas.
output_columns = [
    "spend",
    "signups",
    "avg_3wk_spend",
    "spend_per_signup",
    "spend_zero_mean_unit_variance",
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df)

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
# dr.visualize_execution(output_columns, './my_dag.dot', {})
# dr.display_all_functions('./my_full_dag.dot')
