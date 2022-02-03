import logging
import typing

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import dataframe
import numpy as np


from hamilton import base
from hamilton import node

logger = logging.getLogger(__name__)


class KoalasDataFrameResult(base.ResultMixin):
    """Mixin for building a koalas dataframe from the result"""

    @staticmethod
    def build_result(**columns: typing.Dict[str, typing.Any]) -> pd.DataFrame:
        """TODO figure out how to best signal that we want a koalas DF result."""
        pass


class SparkKoalasGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Spark with Koalas.

    Currently this class assumes you're running SPARK 3.2+.

    Some tips on koalas (before it was merged into spark 3.2):
     - https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, spark_session, result_builder: base.ResultMixin, spine_column: str):
        """Constructor

        :param spark_session: the spark session to use
        :param result_builder: the function to build the result -- currently on Pandas and Koalas are "supported".
        :param spine_column: the column we should use first as the spine and then subsequently join against.
        """
        self.spark_session = spark_session
        if not (isinstance(result_builder, base.PandasDataFrameResult) or
                isinstance(result_builder, KoalasDataFrameResult)):
            raise ValueError('SparkKoalasGraphAdapter only supports returning:'
                             ' a "pandas" DF at the moment, or a "koalas" DF at the moment.')
        self.result_builder = result_builder
        self.spine_column = spine_column

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        # TODO: flesh this out more
        if (node_type == pd.Series or node_type == ps.Series) and (
                isinstance(input_value, ps.DataFrame) or isinstance(input_value, ps.Series)):
            return True
        elif node_type == np.array and isinstance(input_value, dataframe.DataFrame):
            return True

        return node_type == typing.Any or isinstance(input_value, node_type)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        if node_type == ps.Series and input_type == pd.Series:
            return True
        elif node_type == pd.Series and input_type == ps.Series:
            return True
        elif node_type == ps.DataFrame and input_type == pd.DataFrame:
            return True
        elif node_type == pd.DataFrame and input_type == ps.DataFrame:
            return True
        return node_type == input_type

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return node.callable(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Union[pd.DataFrame, ps.DataFrame]:
        # we don't use the actual function for building right now, we use this hacky equivalent
        df = ps.DataFrame(columns[self.spine_column])
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
            df[k] = v
        if isinstance(self.result_builder, base.PandasDataFrameResult):
            return df.to_pandas()
        else:
            return df
