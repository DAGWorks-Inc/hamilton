from typing import Any, Dict, Union

import polars as pl

from hamilton import base


class PolarsDataFrameResult(base.ResultMixin):
    """We need to create a result builder for our use case. Hamilton doesn't have a standard one
    to use just yet. If you think it should -- let's chat and figure out a way to make it happen!
    """

    def build_result(
        self, **outputs: Dict[str, Union[pl.Series, pl.DataFrame, Any]]
    ) -> pl.DataFrame:
        """This is the method that Hamilton will call to build the final result. It will pass in the results
        of the requested outputs that you passed in to the execute() method.
        :param outputs: The results of the requested outputs.
        :return: a polars DataFrame.
        """
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, pl.DataFrame):  # it's a dataframe
                return value
            elif not isinstance(value, pl.Series):  # it's a single scalar/object
                key, value = outputs.popitem()
                return pl.DataFrame({key: [value]})
            else:  # it's a series
                return pl.DataFrame(outputs)
        # TODO: check for length of outputs and determine what should
        # happen for mixed outputs that include scalars for example.
        return pl.DataFrame(outputs)
