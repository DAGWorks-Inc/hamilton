import logging
import typing

import numpy as np
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import dataframe

from hamilton import base, node
from hamilton.base import SimplePythonGraphAdapter

logger = logging.getLogger(__name__)


class KoalasDataFrameResult(base.ResultMixin):
    """Mixin for building a koalas dataframe from the result"""

    @staticmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> ps.DataFrame:
        """Right now this class is just used for signaling the return type."""
        pass


class SparkKoalasGraphAdapter(base.HamiltonGraphAdapter, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Spark with Koalas.

    Use `pip install sf-hamilton[spark]` to get the dependencies required to run this.

    Currently this class assumes you're running SPARK 3.2+.

    Some tips on koalas (before it was merged into spark 3.2):
     - https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
     - https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html

    Spark is a more heavyweight choice to scale computation for Hamilton graphs creating a Pandas Dataframe.

    # Notes on scaling:
      - Multi-core on single machine ✅ (if you setup Spark locally to do so)
      - Distributed computation on a Spark cluster ✅
      - Scales to any size of data as permitted by Spark ✅

    # Function return object types supported:
     - ⛔ Not generic. This does not work for every Hamilton graph.
     - ✅ Currently we're targeting this at Pandas/Koalas types [dataframes, series].

    # Pandas?
     - ✅ Koalas on Spark 3.2+ implements a good subset of the pandas API. Keep it simple and you should be good to go!

    DISCLAIMER -- this class is experimental, so signature changes are a possibility!
    """

    def __init__(self, spark_session, result_builder: base.ResultMixin, spine_column: str):
        """Constructor

        :param spark_session: the spark session to use
        :param result_builder: the function to build the result -- currently on Pandas and Koalas are "supported".
        :param spine_column: the column we should use first as the spine and then subsequently join against.
        """
        self.spark_session = spark_session
        if not (
            isinstance(result_builder, base.PandasDataFrameResult)
            or isinstance(result_builder, KoalasDataFrameResult)
            or isinstance(result_builder, base.DictResult)
        ):
            raise ValueError(
                "SparkKoalasGraphAdapter only supports returning:"
                ' a "pandas" DF at the moment, a "koalas" DF at the moment, or a "dict" of results.'
            )
        self.result_builder = result_builder
        self.spine_column = spine_column

    @staticmethod
    def check_input_type(node_type: typing.Type, input_value: typing.Any) -> bool:
        """Function to equate an input value, with expected node type.

        We need this to equate pandas and koalas objects/types.

        :param node_type: the declared node type
        :param input_value: the actual input value
        :return: whether this is okay, or not.
        """
        # TODO: flesh this out more
        if (node_type == pd.Series or node_type == ps.Series) and (
            isinstance(input_value, ps.DataFrame) or isinstance(input_value, ps.Series)
        ):
            return True
        elif node_type == np.array and isinstance(input_value, dataframe.DataFrame):
            return True

        return SimplePythonGraphAdapter.check_input_type(node_type, input_value)

    @staticmethod
    def check_node_type_equivalence(node_type: typing.Type, input_type: typing.Type) -> bool:
        """Function to help equate pandas with koalas types.

        :param node_type: the declared node type.
        :param input_type: the type of what we want to pass into it.
        :return: whether this is okay, or not.
        """
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
        """Function that is called as we walk the graph to determine how to execute a hamilton function.

        :param node: the node from the graph.
        :param kwargs: the arguments that should be passed to it.
        :return: returns a koalas column
        """
        return node.callable(**kwargs)

    def build_result(
        self, **outputs: typing.Dict[str, typing.Any]
    ) -> typing.Union[pd.DataFrame, ps.DataFrame, dict]:
        if isinstance(self.result_builder, base.DictResult):
            return self.result_builder.build_result(**outputs)
        # we don't use the actual function for building right now, we use this hacky equivalent
        df = ps.DataFrame(outputs[self.spine_column])
        for k, v in outputs.items():
            logger.info(f"Got column {k}, with type [{type(v)}].")
            df[k] = v
        if isinstance(self.result_builder, base.PandasDataFrameResult):
            return df.to_pandas()
        else:
            return df
