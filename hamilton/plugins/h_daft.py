from typing import Any, Dict, Type, Union

import daft

from hamilton import base


class DaftDataFrameResult(base.ResultMixin):
    """A ResultBuilder that produces a polars dataframe.

    Use this when you want to create a polars dataframe from the outputs. Caveat: you need to ensure that the length
    of the outputs is the same, otherwise you will get an error; mixed outputs aren't that well handled.

    To use:

    .. code-block:: python

        from hamilton import base, driver
        from hamilton.plugins import polars_extensions
        polars_builder = polars_extensions.PolarsDataFrameResult()
        adapter = base.SimplePythonGraphAdapter(polars_builder)
        dr =  driver.Driver(config, *modules, adapter=adapter)
        df = dr.execute([...], inputs=...)  # returns polars dataframe

    Note: this is just a first attempt at something for Polars. Think it should handle more? Come chat/open a PR!
    """

    def build_result(
        self, **outputs: Dict[str, Union[daft.dataframe.dataframe.DataFrame, Any]]
    ) -> daft.DataFrame:
        """This is the method that Hamilton will call to build the final result. It will pass in the results
        of the requested outputs that you passed in to the execute() method.

        Note: this function could do smarter things; looking for contributions here!

        :param outputs: The results of the requested outputs.
        :return: a polars DataFrame.
        """
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, daft.DataFrame):  # it's a dataframe
                return value
            elif not isinstance(value, daft.DataFrame):  # it's a single scalar/object
                key, value = outputs.popitem()
                return daft.DataFrame({key: [value]})
            else:  # it's a series
                return daft.DataFrame(outputs)
        # TODO: check for length of outputs and determine what should
        # happen for mixed outputs that include scalars for example.
        return daft.DataFrame(outputs)

    def output_type(self) -> Type:
        return daft.DataFrame
