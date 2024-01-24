import logging
import sys
from typing import Any, Dict, Type, Union

import daft

from hamilton import base, driver

logging.basicConfig(stream=sys.stdout)


class DaftDataFrameResult(base.ResultMixin):
    def build_result(
        self, **outputs: Dict[str, Union[daft.dataframe.dataframe.DataFrame, Any]]
    ) -> daft.dataframe.dataframe.DataFrame:
        """This is the method that Hamilton will call to build the final result. It will pass in the results
        of the requested outputs that you passed in to the execute() method.

        Note: this function could do smarter things; looking for contributions here!

        :param outputs: The results of the requested outputs.
        :return: a polars DataFrame.
        """
        # TODO:
        # do dataframe to dataframe
        # cannot pull expressions
        # do single column dataframes
        #
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, daft.dataframe.dataframe.DataFrame):  # it's a dataframe
                return value
            elif not isinstance(
                value, daft.expressions.expressions.Expression
            ):  # it's a single scalar/object
                key, value = outputs.popitem()
                return daft.dataframe.dataframe.DataFrame({key: [value]})
            else:  # it's a series
                return daft.dataframe.dataframe.DataFrame(outputs)
        # TODO: check for length of outputs and determine what should
        # happen for mixed outputs that include scalars for example.
        return daft.from_pydict(outputs)

    def output_type(self) -> Type:
        return daft.dataframe.dataframe.DataFrame


# Create a driver instance.
adapter = base.SimplePythonGraphAdapter(result_builder=DaftDataFrameResult())
config = {
    "base_df_location": "dummy_value",
}
import functions  # where our functions are defined

dr = driver.Driver(config, functions, adapter=adapter)
# note -- currently the result builder does not handle mixed outputs, e.g. Series and scalars.
output_columns = [
    "base_df",
    # "signups",
    "avg_3wk_spend",
    "spend_per_signup",
    "spend_zero_mean_unit_variance",
]
# let's create the dataframe!
df = dr.execute(output_columns)
print(df)

# To visualize do `pip install "sf-hamilton[visualization]"` if you want these to work
# dr.visualize_execution(output_columns, './polars', {"format": "png"})
# dr.display_all_functions('./my_full_dag.dot')
