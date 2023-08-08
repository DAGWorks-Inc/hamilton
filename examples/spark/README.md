
# Scaling Hamilton on Spark
## Pyspark

If you're using pyspark, Hamilton allows for natural manipulation of pyspark dataframes,
with some special constructs for managing DAGs of UDFs.

See the example in `pyspark` to learn more.

## Pandas
If you're using Pandas, Hamilton scales by using Koalas on Spark.
Koalas became part of Spark officially in Spark 3.2, and was renamed Pandas on Spark.
The example in `pandas_on_spark` here assumes that.

## Pyspark UDFs
If you're not using Pandas, then you can use Hamilton to manage and organize your pyspark UDFs.
See the example in `pyspark_udfs`.

Note: we're looking to expand coverage and support for more Spark use cases. Please come find us, or open an issue,
if you have a use case that you'd like to see supported!
