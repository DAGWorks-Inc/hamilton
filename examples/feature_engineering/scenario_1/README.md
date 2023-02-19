# Scenario 1: the simple case - ETL + Online API
Assumptions:
1. you have an ETL process to create features that ingests raw data, and then creates a training set for you to train on.
2. you have an online API from where you want to serve the predictions, and you can provide the same raw data to it that,
you would have access to in your ETL process.

## ETL Process
ETL stands for Extract, Transform, Load. This is the process of taking raw data, transforming it into features,
and then loading the data somewhere/doing something with it. E.g. you pull raw data, transform it into features,
and then create a training set with which to train a model. This is exactly what we have in this example and is
what etl.py orchestrates; it is however not complete (i.e. doesn't save, or fit a model) and is just illustrative
for this example.

### File Descriptions
Here is a description of all the files and what they do.
Note: aggregation features, like `mean()` or `std_dev()` make sense only in an
offline setting where you have all the data. In an online setting, computing them
does not make sense (probably). In `etl.py` there is a note that you need to store `age_mean` and
`age_std_dev` and then somehow get those values to plug into the code in `fastapi_server.py`.
If you're getting started, these could be hardcoded values, or stored to a file that
is loaded much like the model, or queried from a database, etc. Though you'll want
to ensure these values match whatever values the model was trained with.

#### offline_loader.py
Contains logic to load raw data. Here it's a flat file, but it could be going
to a database, etc.

#### features.py
The feature transform logic that takes raw data and transforms it into features. It contains some runtime
dataquality checks using Pandera.

Important not, there are two aggregations features defined: `age_mean` and `age_std_dev`, that are computed on the
`age` column. These make sense to compute in an offline setting as you have all the data, but in an online setting where
you'd be performing inference, that doesn't makse sense. So for the online case, these computations be "overridden" in
`fastapi_server.py` with the values that were computed in the offline setting that you have stored (as mentioned above
and below it's up to you how to store them/sync them).

#### etl.py
This script mimics what one might do to fit a model: extract data, transform into features,
and then load features somewhere or fit a model. It's pretty basic and is meant
to be illustrative. It is not complete, i.e. doesn't save, or fit a model, it just extracts and transforms data
into features to create a dataframe.

As seen in this image of what is executed - we see the that data is pulled from a data source, and transformed into features.
![offline execution](offline_execution.dot.png)

#### constants.py
Rather than hardcoding what features the model should have in two places, we define
it in a single place and import it where needed; this is simple if you can share the code eaisly.
However, this is something you'll have to determine how to best do in your set up. There are many ways to do this,
come ask in the slack channel if you need help.

#### fastapi_server.py
The FastAPI server that serves the predictions. It's pretty basic and is meant to
illustrate the steps of what's required to serve a prediction from a model, where
you want to use the same feature computation logic as in your ETL process.

Note: the aggregation feature values are provided at run time and are the same
for all predictions -- how you "link" or "sync" these values to the webservice & model
is up to you; in this example we just hardcode them.

Here is the DAG that is executed when a request is made. As you can see, no data is loaded, as we're assuming
that data comes from the API request. Note: `age_mean` and `age_std_dev` are overridden with values and would
not be executed (our visualization doesn't take into account overrides just yet).
![online execution](online_execution.dot.png)
