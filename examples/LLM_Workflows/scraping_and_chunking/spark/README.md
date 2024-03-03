# PySpark version of the scraping and chunking example

Here we show how you can integrate most of the Hamilton dataflow, that we defined previously
easily into a PySpark job. This is useful if you want to run the same dataflow on a larger dataset,
or have to run it on a cluster. Importantly this means you don't have to rewrite your
code, or have to change where/how you develop!


# Changes
Pyspark compatible column types.
 - e.g. objects -> to strings or structs.
