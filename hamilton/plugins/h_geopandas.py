from typing import Any, Dict, Type, Union

import geopandas as gpd

from hamilton import base


class GeoPandasDataFrameResult(base.ResultMixin):
    """A ResultBuilder that produces a geopandas dataframe.

    Use this when you want to create a geopandas dataframe from the outputs. Caveat: you need to ensure that the length
    of the outputs is the same, otherwise you will get an error; mixed outputs aren't that well handled.

    To use:

    .. code-block:: python

        from hamilton import base, driver
        from hamilton.plugins import geopandas_extensions
        polars_builder = polars_extensions.GeopandasDataFrameResult()
        adapter = base.SimplePythonGraphAdapter(geopandas_builder)
        dr =  driver.Driver(config, *modules, adapter=adapter)
        df = dr.execute([...], inputs=...)  # returns geopandas dataframe

    Note: this is just a first attempt at something for Geopandas. Think it should handle more? Come chat/open a PR!
    """

    def build_result(
        self, **outputs: Dict[str, Union[gpd.GeoSeries, gpd.GeoDataFrame, Any]]
    ) -> gpd.GeoDataFrame:
        """This is the method that Hamilton will call to build the final result. It will pass in the results
        of the requested outputs that you passed in to the execute() method.

        Note: this function could do smarter things; looking for contributions here!

        :param outputs: The results of the requested outputs.
        :return: a geopandas DataFrame.
        """
        if len(outputs) == 1:
            (value,) = outputs.values()  # this works because it's length 1.
            if isinstance(value, gpd.GeoDataFrame):  # it's a dataframe
                return value
            elif not isinstance(value, gpd.GeoSeries):  # it's a single scalar/object
                key, value = outputs.popitem()
                return gpd.GeoDataFrame({key: [value]})
            else:  # it's a series
                return gpd.GeoDataFrame(outputs)
        # TODO: check for length of outputs and determine what should
        # happen for mixed outputs that include scalars for example.
        return gpd.GeoDataFrame(outputs)

    def output_type(self) -> Type:
        return gpd.GeoDataFrame
