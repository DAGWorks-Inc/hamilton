# Hamilton and Spark

Hamilton now has first-class pyspark integration. While we will likely be improving it as we go along,
this version is the first we're considering "stable" and moving out of "experimental"

# Design

The idea is to break your code into components. These components make one of two shapes:

1. Run linearly (e.g. chained dataframe transformations -- aggregations, etc...)
2. Form a DAG of operations (E.G. a set of features that have inter-feature dependencies)

For the first case, we just use the pyspark dataframe API. You define functions that, when put
through Hamilton, act as a pipe. For example:

```python
import pyspark.sql as ps

def raw_data_1() -> ps.DataFrame:
    """Loads up data from an external source"""

def raw_data_2() -> ps.DataFrame:
    """Loads up data from an external source"""

def all_initial_data(raw_data_1: ps.DataFrame, raw_data_2: ps.DataFrame) -> ps.DataFrame:
    """Combines the two dataframes"""
    return _join(raw_data_1, raw_data_2)

def raw_data_3() -> ps.DataFrame:
    """Loads up data from an external source"""
```

For the next case, we define transformations that are columnar/map-oriented in nature.
These are UDFs (either pandas or python) that get applied to the dataframe in a specific order:

```python
import pandas as pd

#map_transforms.py

def column_3(column_1_from_dataframe: pd.Series) -> pd.Series:
    """Transforms column 1 (from the dataframe) into column 3"""
    return _some_transform(column_1_from_dataframe)

def column_4(column_2_from_dataframe: pd.Series) -> pd.Series:
    """Transforms column 2 (from the dataframe) into column 4"""
    return _some_other_transform(column_2_from_dataframe)

def column_5(column_3: pd.Series, column_4: pd.Series) -> pd.Series:
    """is a combination of column_1_from_dataframe and column_2_from_dataframe"""
    return _yet_another_transform(column_3, column_4)
```

Finally, we combine them together with a call to `with_column`:

```python
from hamilton.experimental.h_spark import with_columns
import pyspark.sql as ps
import map_transforms # file defined above

@with_columns(
    map_transforms, # Load all the functions we defined above
    select=["column_1_from_dataframe", "column_2_from_dataframe"], # calculate these
    dataframe="all_initial_data"  # use this dataframe as the source (and inject the result into the final_result function
)
def final_result(all_initial_data: ps.DataFrame, raw_data_3: ps.DataFrame) -> ps.DataFrame:
    """Gives the final result. This decorator will apply the transformations in the order.
    Then, the final_result function is called, with the result of the transformations passed in."""
    return _join(all_initial_data, raw_data_3)
```

Thus you can represent a clean, modular, unit-testable string of transformations while also allowing for
complex sets of feature UDFs. Note that we will shortly allow the with_columns group to include pyspark
functions (dataframe -> dataframe) as well, but that is not currently supported.

We have implemented the hamilton hello_world example in [run.py](run.py) and the [map_transforms.py](map_transforms.py)/[dataflow.py](dataflow.py) files
so you can compare. You can run `run.py`:

`python run.py`

and check out the interactive example in the `notebook.ipynb` file.

## How does this work?

The `with_columns` decorator does the following:
1. Resolves the functions you pass in, with the config passed from the driver
2. Transforms them each into a node, in topological order.
   - Retains all specified dependencies
   - Adds a single dataframe that gets wired through (linearizing the operations)
   - Transforms each function into a function of that input dataframe and any other external dependencies

Thus the graph continually assigns to a single (immutable) dataframe, tracking the result, and still displays the DAG shape
that was presented by the code. Column-level lineage is preserved and readable from the code, while it executes as a
normal set of spark operations.

## Why use Hamilton and not plain spark?

As you can see above, we delegate almost entirely to spark. However, when you want column-level lineage and modular functions,
vanilla spark is often suboptimal. It requires "linearization" (e.g. chained) modification of the same dataframe, and can
get messy and out of hand. Thus we group together columnar (map-ish) operations together, while still allowing the
chaining of dataframe functions that pyspark expresses naturally.

Furthermore, this opens up a few interesting debugging capabilities, that we're building out:
1. Running components of your workflow in pandas
2. Unit testing individual spark transforms
3. Grabbing intermediate results to debug the spark execution plan at any given moment
4. Adding `collect` halfway through to make inspection easier
5. Breaking large spark jobs into separate tasks by arranging their functions into modules

We have found that spark ETLs tend to fit the patterns above nicely, and that hamilton helps make them shine.

## Why not just pass around spark dataframes in Hamilton functions?

This is great when your functions are linear, but when you have enough features this gets messy. Specifically,
unit testing/zooming in on transformations often requires tooling to manage/collect UDFs, and that's what Hamilton does.

The biggest problem comes when you want column-level transformations so you extract out the columns into different dataframes
and join them together. This can result in (accidental) performance issues, as spark does very poorly in handling multiple
large joins.

## Why not use pandas-on-spark?

We support that! You can use pandas-on-spark with the `KoalaGraphAdapter` -- see [Pandas on Spark](../pandas_on_spark/README.md) for reference.
Some people prefer vanilla spark, some like pandas-on-spark. We support both.

Note there are other scaling libraries that Hamilton supports -- it all depends on your use-case:

- [dask](../../dask/README.md)
- [ray](../../ray/README.md)
- [modin](https://github.com/modin-project/modin) (no example for modin yet but it is just the pandas API with a different import)
