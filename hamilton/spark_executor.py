import logging
import typing
from types import ModuleType

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import dataframe
from pyspark.sql.functions import pandas_udf
import numpy as np


from . import node, base, driver

logger = logging.getLogger(__name__)


class SparkExecutorKoalas(base.HamiltonExecutor, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Spark with Koalas"""

    def __init__(self, spark_session, result_builder: base.ResultMixin, initial_df):
        self.spark_session = spark_session
        if not isinstance(result_builder, base.PandasDataFrameResult):
            raise ValueError('SparkExecutorKoalas only supports returning a "pandas" DF at the moment.')
        self.initial_df = initial_df

    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        # NOTE: the type of ray  is unknown until they are computed
        print(type(input))
        # if isinstance(input, pyspark.):
        #     return True
        if (node.type == pd.Series or node.type == ps.Series) and (
                isinstance(input, dataframe.DataFrame) or isinstance(input, ps.Series)):
            return True
        elif node.type == np.array and isinstance(input, dataframe.DataFrame):
            return True

        return node.type == typing.Any or isinstance(input, node.type)

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        return node.callable(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Any:

        df = self.initial_df
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
            df[k] = v
        return df


class SparkExecutorUDFs(base.HamiltonExecutor, base.ResultMixin):
    """Class representing what's required to make Hamilton run on Spark with UDFs"""

    def __init__(self, spark_session, result_builder: base.ResultMixin, spine_column_name):
        self.spark_session = spark_session
        if isinstance(result_builder, base.PandasDataFrameResult):
            self.result_type = 'pandas'
        else:
            self.result_type = 'spark'
        self.spine_column_name = spine_column_name

    def check_input_type(self, node: node.Node, input: typing.Any) -> bool:
        # NOTE: the type of ray  is unknown until they are computed
        print(type(input))
        # if isinstance(input, pyspark.):
        #     return True
        if node.type == pd.Series and isinstance(input, dataframe.DataFrame):
            return True
        elif node.type == np.array and isinstance(input, dataframe.DataFrame):
            return True

        return node.type == typing.Any or isinstance(input, node.type)

    def execute_node(self, node: node.Node, kwargs: typing.Dict[str, typing.Any]) -> typing.Any:
        """
        Assumptions:
         - every "input" has an "index" column
         - we need to figure out what to pass into the function from the order of the node dependencies
           (I don't think we can depend on the dictionary iteration order?)
         - we need to wrap the function in a UDF
         - also need to think about scalars versus vectors?

         Problems to overcome:
          - data coming from different sources
          - and thus how to join things appropriately -- if single input -- good to go, if more than one --
            -- assume equal indexes? or?
        """

        return node.callable(**kwargs)

    def build_result(self, **columns: typing.Dict[str, typing.Any]) -> typing.Any:

        df = columns[self.spine_column_name]
        for k, v in columns.items():
            logger.info(f'Got column {k}, with type [{type(v)}].')
            df.join()
        if self.result_type == 'pandas':
            return df.toPandas()
        else:
            print(df.show())
            return df



class SparkDriver(driver.Driver):

    def __init__(self,
                 DAG_config: typing.Dict[str, typing.Union[str, int, bool, float]],
                 initial_columns: typing.Dict[str, typing.Union[list, pd.Series, pd.DataFrame, np.ndarray]],
                 *modules: ModuleType,
                 spark_parameters: typing.Dict[str, typing.Any],
                 result_builder: base.ResultMixin):
        if spark_parameters.get('convert_to_dask_data_type', True):
            config = self.sparkify_data(spark_parameters, initial_columns)
        else:
            config = initial_columns.copy()  # shallow copy
        config: typing.Dict[str, typing.Any]  # can now become anything...
        errors = []
        for scalar_key in DAG_config.keys():
            if scalar_key in config:
                errors.append(f'\nError: {scalar_key} already defined in vector_config.')
            else:
                config[scalar_key] = DAG_config[scalar_key]
        if errors:
            raise ValueError(f'Bad configs passed:\n [{"".join(errors)}].')
        super(SparkDriver, self).__init__(config, *modules, executor=SparkExecutorKoalas(result_builder))

    @staticmethod
    def sparkify_data(ray_parameters, vector_config):
        """Makes things into ray data types."""
        # TODO:
        return vector_config

    def execute(self,
                final_vars: typing.List[str],
                overrides: typing.Dict[str, typing.Any] = None,
                display_graph: bool = False) -> pd.DataFrame:
        columns = self.raw_execute(final_vars, overrides, display_graph)
        return self.executor.build_result(**columns)



