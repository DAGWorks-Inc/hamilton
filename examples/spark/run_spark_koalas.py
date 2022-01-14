import importlib
import logging
import sys

import pandas as pd
import pyspark.pandas as ps
from hamilton import driver, base, spark_executor

from pyspark.sql import SparkSession
from pyspark.sql import types

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    module_name = 'my_functions'
    module = importlib.import_module(module_name)

    spark = SparkSession.builder.getOrCreate()
    ps.set_option('compute.ops_on_diff_frames', True)  # we should play around here on how to correctly initialize data.
    ps.set_option('compute.default_index_type', 'distributed') # this one doesn't seem to work?
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    initial_columns = {  # load from actuals or wherever -- this is our initial data we use as input.
        # 'signups': pd.Series([1, 10, 50, 100, 200, 400], name='signups'),
        'signups': ps.from_pandas(pd.Series([1, 10, 50, 100, 200, 400], name='signups')),
        # 'spend': pd.Series([10, 10, 20, 40, 40, 50], name='spend'),
        'spend': ps.from_pandas(pd.Series([10, 10, 20, 40, 40, 50], name='spend')),
    }
    df_container = ps.DataFrame(initial_columns['spend'])  # assign spine column
    # Proving to myself that koalas works:
    # print(df_container)
    # import my_functions
    # df_container['spend_per_signup'] = my_functions.spend_per_signup(df_container['spend'], initial_columns['signups'])
    # df_container['avg_3wk_spend'] = my_functions.avg_3wk_spend(df_container['spend'])
    # df_container['spend_zero_mean_unit_variance'] = my_functions.spend_zero_mean_unit_variance(
    #     my_functions.spend_zero_mean(df_container['spend'], my_functions.spend_mean(df_container['spend'])),
    #     my_functions.spend_std_dev(df_container['spend'])
    # )
    # df_container['signups'] = initial_columns['signups']
    # print(df_container)
    """
   spend  spend_per_signup  avg_3wk_spend  spend_zero_mean_unit_variance  signups
0     10            10.000            NaN                      -1.064405        1
1     10             1.000            NaN                      -1.064405       10
2     20             0.400      13.333333                      -0.483821       50
3     40             0.400      23.333333                       0.677349      100
4     40             0.200      33.333333                       0.677349      200
5     50             0.125      43.333333                       1.257934      400
    """
    # okay hamilton is now doing its thing:
    se = spark_executor.SparkExecutorKoalas(spark_session=spark, result_builder=base.PandasDataFrameResult(),
                                            initial_df=df_container)
    dr = driver.Driver(initial_columns, module, executor=se)  # can pass in multiple modules
    # we need to specify what we want in the final dataframe.
    output_columns = [
        'spend',
        'signups',
        'avg_3wk_spend',
        'spend_per_signup',
        'spend_zero_mean_unit_variance'
    ]
    # let's create the dataframe!
    df = dr.execute(output_columns, display_graph=True)
    print(df)
    spark.stop()
    """
       spend  signups  avg_3wk_spend  spend_per_signup  spend_zero_mean_unit_variance
0     10        1            NaN            10.000                      -1.064405
1     10       10            NaN             1.000                      -1.064405
2     20       50      13.333333             0.400                      -0.483821
3     40      100      23.333333             0.400                       0.677349
4     40      200      33.333333             0.200                       0.677349
5     50      400      43.333333             0.125                       1.257934
    """
