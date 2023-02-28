# Scaling Hamilton on Spark

## Pandas
If you're using Pandas, Hamilton scales by using Koalas on Spark.
Koalas became part of Spark officially in Spark 3.2, and was renamed Pandas on Spark.
The example in `pandas_on_spark` here assumes that.

## Pyspark UDFs
If you're not using Pandas, then you can use Hamilton to manage and organize your pyspark UDFs.
See the example in `pyspark_udfs`.
