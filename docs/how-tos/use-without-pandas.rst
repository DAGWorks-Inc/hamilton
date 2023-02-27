===========================
Use Hamilton without Pandas
===========================

As we made clear earlier, Making use of Hamilton does not require that you utilize Pandas.
Not only can hamilton functions output any valid python object, but Hamilton also naturally integrates
with a few dataframe libraries.

In this example, we rebuild the hello_world example using the `polars <https://www.pola.rs/>`_ library.

https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/polars.

Note that we are currently working on other examples, including one for pyspark
(Hamilton already has native `pandas-on-spark <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/spark>`_ support).
