import importlib
import logging
import sys
from functools import partial

import pandas as pd
from pyspark.sql import functions as F
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import types

from hamilton import base, spark_executor, driver

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    module_name = 'my_functions'
    module = importlib.import_module(module_name)

    spark = SparkSession.builder.getOrCreate()
    ps.set_option('compute.ops_on_diff_frames', True)  # we should play around here on how to correctly initialize data.
    ps.set_option('compute.default_index_type', 'distributed')  # this one doesn't seem to work?
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    signups = spark.createDataFrame(pd.DataFrame(
        {'signups': [1, 10, 50, 100, 200, 400],
         'index': [0, 1, 2, 3, 4, 5]}
    ))
    spend = spark.createDataFrame(pd.DataFrame(
        {'spend': [10, 10, 20, 40, 40, 50],
         'index': [0, 1, 2, 3, 4, 5]}
    ))
    initial_columns = {'signups': signups, 'spend': spend}

    df_container = initial_columns['spend']  # assign spine column

    # Manually doing things to understand what needs to happen:
    import my_functions

    s_p_s = F.pandas_udf(my_functions.spend_per_signup, types.DoubleType())
    a_3_s = F.pandas_udf(my_functions.avg_3wk_spend, types.DoubleType())
    # s_z_m_u_v = F.pandas_udf(my_functions.spend_zero_mean_unit_variance, types.DoubleType())
    s_z_m_u_v = F.udf(my_functions.spend_zero_mean_unit_variance, types.DoubleType())
    # s_z_m = F.pandas_udf(my_functions.spend_zero_mean, types.DoubleType())
    s_z_m = F.udf(my_functions.spend_zero_mean, types.DoubleType())
    s_s_d = F.pandas_udf(my_functions.spend_std_dev, types.DoubleType())
    s_m = F.pandas_udf(my_functions.spend_mean, types.DoubleType())
    print(df_container.show())
    print(initial_columns['signups'].show())
    df_container = df_container.join(initial_columns['signups'], on=['index'])
    df_container = df_container.withColumn('spend_per_signup', s_p_s(df_container['spend'], df_container['signups']))
    df_container = df_container.withColumn('avg_3wk_spend', a_3_s(df_container['spend']))
    # df_container = df_container.withColumn('spend_mean', s_m(df_container['spend']))
    spend_std_dev = df_container.select(s_s_d(df_container['spend']).alias('spend_std_dev')).first().asDict()['spend_std_dev']
    spend_mean = df_container.select(s_m(df_container['spend']).alias('spend_mean')).first().asDict()['spend_mean']
    print(spend_mean)
    print(spend_std_dev)
    print(df_container.show())
    # print(F.first(spend_mean.first()), F.first(spend_std_dev))
    """I probably need to partial the function with these values? else spark doesn't know how to join -- or we """
    # partial()
    df_container = df_container.withColumn('spend_zero_mean', s_z_m(df_container['spend'], F.lit(spend_mean)))
    df_container = df_container.withColumn('spend_zero_mean_unit_variance', s_z_m_u_v(
        df_container['spend_zero_mean'],
        F.lit(spend_std_dev)
    ))
    df_container = df_container.select(
        *[F.col(c) for c in
          ['spend',
           'signups',
           'avg_3wk_spend',
           'spend_per_signup',
           'spend_zero_mean_unit_variance'] + ['index']
          ]
    )
    df = df_container.toPandas()
    df.index = df['index']
    del df['index']
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df.to_string())
    """
       spend  signups  avg_3wk_spend  spend_per_signup  spend_zero_mean_unit_variance
index                                                                                
0         10        1            NaN            10.000                      -1.064405
1         10       10            NaN             1.000                      -1.064405
2         20       50      13.333333             0.400                      -0.483821
3         40      100      23.333333             0.400                       0.677349
4         40      200      33.333333             0.200                       0.677349
5         50      400      43.333333             0.125                       1.257934
    """

    # okay hamilton is now doing its thing:
    # se = spark_executor.SparkExecutorUDFs(spark_session=spark,
    #                                       result_builder=base.PandasDataFrameResult(),
    #                                       spine_column_name='spend')
    # dr = driver.Driver(initial_columns, module, executor=se)  # can pass in multiple modules
    # # we need to specify what we want in the final dataframe.
    # output_columns = [
    #     'spend',
    #     'signups',
    #     'avg_3wk_spend',
    #     'spend_per_signup',
    #     'spend_zero_mean_unit_variance'
    # ]
    # # let's create the dataframe!
    # df = dr.execute(output_columns, display_graph=True)
    # print(df)
    spark.stop()
