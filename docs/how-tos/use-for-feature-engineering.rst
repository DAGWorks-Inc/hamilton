==========================================
Use Hamilton for Feature Engineering
==========================================

Hamilton's roots are in time-series offline feature engineering. But it can be used for any type of feature engineering:
offline, streaming, online. All our examples are oriented towards Pandas, but rest assured, you can use Hamilton with
any python objects, e.g. numpy, polars, and even pyspark.

Here's a 20 minute video (`slides <https://github.com/skrawcz/talks/files/9759661/FS.Summit.2022.-.Hamilton.pdf>`__), with
brief backstory on Hamilton, and an overview (at around the 8:52 mark) of how to use it for feature engineering which
was presented at the Feature Store Summit 2022:

.. raw:: html

    <iframe width="560" height="315" src="https://www.youtube.com/embed/b9tfdNZZ-nk" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

Otherwise here we present a high level overview and then direct users to the examples folder for more details. We suggest
reading the Offline Feature Engineering section first, since it's the most common use case, and helps explain the
python module structure you should be going for with Hamilton. If you need more guidance here, please reach out to us on
`slack <https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg>`__.


Offline Feature Engineering
---------------------------
To use Hamilton for offline feature engineering, a common pattern is:

1. create a data_loader module(s) that loads the data from the source(s) (e.g. a database, a csv file, etc.).
2. create feature transform module(s) that transform the data into features.
3. create a data set module(s) that combines the data_loader and feature transform modules if you want to connect fitting \
   a model with Hamilton. Or, you do this data set definition in your driver code.

Here is a sketch of the above pattern:

.. code-block:: python

    # data_loader.py
    @extract_columns(*...)  # you can choose to expose individual columns
    def load_data(...) -> pd.DataFrame:
        return pd.read_csv(...)
    ...
    # feature_transform.py
    def feature_a(raw_input_a: pd.Series, ...) -> pd.Series:
        return raw_input_a + ...
    ...
    # dataset.py (optional)
    def model_set_x(feature_a: pd.Series, ...) -> pd.DataFrame:
        return pd.DataFrame({'feature_a': feature_a, ...})
    # run.py
    def main():
        dr = driver.Driver(config, data_loader, feature_transform, dataset)
        feature_df = dr.execute([feature_transform.feature_a, ...])
        ...


Hamilton Example
__________________
We do not provide a specific example here, since most of the examples in the examples folder fall under this category.
Some examples to browse:

* `Hello World <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/hello_world>`__ shows the basics of how to
  use Hamilton.
* `Data Quality <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/data_quality>`__ shows how to incorporate
  runtime data quality checks into your feature engineering pipeline.
* `Time-series Kaggle Example <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/model_examples/time-series>`__
  shows one way to structure your code to ingest, create features, and fit a model.
* `Feature engineering in multiple contexts <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/feature_engineering_multiple_contexts>`__
  helps show how you can use Hamilton in multiple contexts reusing code where possible, e.g. offline, & online.
* `PySpark UDF Map Examples <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/spark/pyspark_udfs>`__
  shows how to use Hamilton to encode map operations for use with PySpark.


Streaming Feature Engineering
-----------------------------
Right now, there is no specific streaming support. Instead, we model the problem as we would for offline. Hamilton
has an `inputs=` argument to the `execute()` function in the driver. This allows you to then instantiate a Hamilton
Driver once, and then call `execute()` multiple times with different inputs. Otherwise you'd have a similar python
module structure as for offline feature engineering -- perhaps just dropping the data_loader module since you would
provide the inputs directly to the `execute()` function.

Here's a sketch of how you might use Hamilton in conjunction with a Kafka Client:

.. code-block:: python

    # run.py
    def main():
        kakfa_client = KafkaClient(...)
        dr = driver.Driver(config, feature_transform)
        for batch in kafka_client.get_batches():  # this is pseudo code, but you get the idea
            feature_df = dr.execute([feature_transform.feature_a, ...], inputs=batch.to_dict())
            # do something / emit back to kafka, etc.


**Caveats to think about**. Here are some things to think about when using Hamilton for streaming feature engineering:

 - aggregation features, you likely want to understand whether you want to aggregate over the entire stream or just \
   the current batch, or load values that were computed offline.


Hamilton Example
__________________
Currently we don't have a streaming example. But we are working on it. We direct users to look at the online example
for now, since conceptually from a modularity stand point, things would be set up in a similar way.

Online Feature Engineering
--------------------------
Online feature engineering can be quite simple or quite complex, depending on your situation. However, good news is,
that Hamilton should be able to help you in any situation. The modularity of Hamilton allows you to swap out implementations
of features easily, as well as override values, and even ask the Driver what features are required from the source data
to create the features that you want. We think Hamilton can help you keep things simple, but then extend to helping you
handle more complex situations.

The basic structure of your python modules, does not change. Depending on whether you want Hamilton to load data from a feature store,
or you have all the data passed in, you just need to appropriately segment your feature transforms into modules, or use
the `@config.*` decorator, to help you segment your feature computation dataflow to give you the flexibility you need.

*Caveats to think about*. Here are some things to think about when using Hamilton for online feature engineering:

 - aggregation features, most likely you'll want to load aggregated feature values that were computed offline, rather \
   than compute them live.

We skip showing a sketch of structure here, and invite you to look at the examples below.

Hamilton Example
__________________
We direct users to look at `Feature engineering in multiple contexts <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/feature_engineering_multiple_contexts>`__
that currently describes two scenarios around how you could incorporate Hamilton into an online web-service, and have
it aligned with your batch offline processes. Note, these examples should give you the high level first principles
view of how to do things. Since having something running in production , we didn't want to get too specific.


FAQ
----

Q. Can I use Hamilton for feature engineering with Feast?
__________________________________________________________
Yes, you can use Hamilton with Feast. See our [Feast example](https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/feast) and accompanying [blog post](https://blog.dagworks.io/p/featurization-integrating-hamilton). Typically people use Hamilton on the offline side to compute features that then
get pushed to Feast. For the online side it varies as to how to integrate the two.
