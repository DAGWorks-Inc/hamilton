# Scenario 1: the simple case - ETL + Online API
Assumptions:
1. you have an ETL process to create features that ingests raw data, and then creates a training set for you to train on.
2. you have an online API from where you want to serve the predictions, and you can provide the same raw data to it that,
you would have access to in your ETL process.

TODO: picture

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
does not make sense. In `etl.py` there is a note that you need to store `age_mean` and
`age_std_dev` and then somehow get those values to plug into the code in `fastapi_server.py`.
If you're getting started, these could be hardcoded values, or stored to a file that
is loaded much like the model, or queried from a database, etc. Though you'll want
to ensure these values match whatever values the model was trained with.

#### offline_loader.py
Contains logic to load raw data. Here it's a flat file, but it could be going
to a database, etc.

#### features.py
The feature transform logic that takes raw data and transforms it into features.

#### etl.py
This script that mimics what one might do to fit a model: extract data, transform into features,
and then load features somewhere or fit a model. It's pretty basic and is meant
to be illustrative.

#### constants.py
Rather than hardcoding what features the model should have in two places, we define
it in a single place and import it where needed.

#### fastapi_server.py
The FastAPI server that serves the predictions. It's pretty basic and is meant to
illustrate the steps of what's required to serve a prediction from a model, where
you want to use the same feature computation logic as in your ETL process.
Note: aggregation features
