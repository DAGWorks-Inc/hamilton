---
description: Executing your DAGs and processing the results
---

# Drivers

Writing functions is great, but its meaningless if you have no way to execute them. Drivers are responsible for the following:

1. Gathering functions in your pipeline
2. Running the DAG
3. Assembling the results

Furthermore, they present an easy way for the user to specify the data she wants without dealing with the complexities of DAGs, function graphs, or nodes.

There are two types of drivers: [framework-specified](drivers.md#framework-specified) and [user-defined](drivers.md#user-defined). this is an active area of development for hamilton -- expect more default drivers, plugin-oriented drivers, and structure around driver offerings to be added soon (and please contribute!).&#x20;

## Framework Specified

These are drivers that come with the framework Their primary goal is to shield the complexity of the DAG and provide a nice clean API. They're cheap, and easy to add to the framework. On the main version of Hamilton we currently have [one driver](https://github.com/stitchfix/hamilton/blob/main/hamilton/driver.py). This runs all the functions in memory and returns the results joined as a pandas Dataframe. We also have multiple experimental distributed drivers (Dask, Spark, and Ray) integrations on the experimental branch.

## User Defined

These are drivers you write yourself! They instantiate a framework-specified drivers and integrate your business logic. They should be cheap, configurable, and have an API tailored to your use case. While the framework-specified driver handles the execution of the DAG, your driver configures it, passes the data, and does whatever you want with the final result. Think side-effects, etc...

