# write features once, use anywhere
A not too uncommon task is that you need to do feature engineering in an offline (e.g. batch via airflow)
setting, as well as an online setting (e.g. synchronous request via FastAPI). What commonly
happens is that the code for features is not shared, and results in two implementations
that result in subtle bugs and hard to maintain code.

With this example we show how you can use Hamilton to:

1. write a feature once.
2. leverage the feature code anywhere that python runs. e.g. in batch and online.
3. show how to modularize components so that if you have values cached in a feature store,
you can inject those values into your feature computation needs.

# Scenarios
We provide two examples for two common scenarios. The example code here tries to be illustrative about
how to think and frame using Hamilton to solve these two scenarios; it contains minimal features so as to not
overwhelm you.

## Scenario 1: the simple case - ETL + Online API
Assume we can get the same raw inputs at prediction time, as it was provided in at training time.
However, we don't want to recompute `age_mean` and `age_std_dev` because recomputing aggregation features
doesn't make sense in an online setting (usually). Instead, we "store" the values for them when we compute features,
and then use those "stored" at prediction time to get the right computation to happen.

## Scenario 2: the more complex case - request doesn't have all the raw data - ETL + Online API
At prediction time we might only have some of the raw data required to compute a prediction. To get the rest
we need to make an API call, e.g. a feature store or a database, that will provide us with that information.
This example shows one way to modularize your Hamilton code so that you can swap out the "source" of the data.
A good exercise would be to make note of the differences with this scenario (2) and scenario 1.

# What next?
Jump into each directory and read the README, it'll explain how the example is set up and how things should work.
