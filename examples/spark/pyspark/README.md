# Hamilton and Spark

Hamilton now has first-class pyspark integration! While we will likely be improving it as we go along,
this version is the first we're considering "stable" and we are planning an imminent move out of "experimental" into "plugins".

# Motivation

Spark is immensely powerful -- its one of the only tools that effectively handles large
datasets in a distributed fashion, and has a variety of plugins/a tooling ecosystem that
data teams love.

Just like pandas pipelines, however, spark pipelines can be difficult to maintain/manage, devolving into
spaghetti code over time. Specifically, we've observed the following problems with pyspark pipelines:
1. They rarely get broken up into modular and  reusable components
2. They contain a lot of "implicit" dependencies -- as you do break them up into functions, it is difficult to
specify which columns the dataframes contain/depend on, and how that changes through the workflow.
3. They are difficult to configure in a readable manner. A monolithic spark script likely has a few different shapes/parameters, and naturally becomes littered with poorly documented if/else statements
4. They're not easy to unit tests. While specific UDFs can be tested, spark transformations are difficult to test in a modular fashion.

Vanilla Hamilton with pyspark gets you part of the way there. You can easily build functions that input/output spark dataframes,
and your code gets more modular/easier to maintain. That said, you still have to deal with the implicit dependencies, and the
fact that you're not really building a DAG of operations -- its more of a linear chain. You don't get any column-level lineage
(you'll have to look at the spark execution plan for that/an external lineage tool), and you either have lots of steps each manipulating
the same dataframe, or a few large modular ones.

The new spark integation is meant to give you the best of both worlds -- we want to allow you to express column-level map (cardinality-preserving) operations
while simultaneously passing around dataframes for aggregations, filters, and joins.


# Design

The idea is to break your code into components. These components make one of two shapes:

1. Run linearly (e.g. cardinality non-preserving operations: aggregations, filters, joins, etc..)
2. Form a DAG of column-level operations (for cardinality-preserving operations)

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
These are UDFs (either pandas or python), or functions of pyspark constructs, that get applied
to the upstream dataframe in a specific order:

```python
import pandas as pd

#map_transforms.py

def column_3(column_1_from_dataframe: pd.Series) -> pd.Series:
    return _some_transform(column_1_from_dataframe)

def column_4(column_2_from_dataframe: pd.Series) -> pd.Series:
    return _some_other_transform(column_2_from_dataframe)

def column_5(column_3: pd.Series, column_4: pd.Series) -> pd.Series:
    return _yet_another_transform(column_3, column_4)
```

Finally, we combine them together with a call to `with_column`:

```python
from hamilton.experimental.h_spark import with_columns
import pyspark.sql as ps
import map_transforms # file defined above

@with_columns(
    map_transforms, # Load all the functions we defined above
    columns_to_pass=[
       "column_1_from_dataframe",
       "column_2_from_dataframe",
       "column_3_from_dataframe"], # use these from the initial datafrmae
)
def final_result(all_initial_data: ps.DataFrame, raw_data_3: ps.DataFrame) -> ps.DataFrame:
    """Gives the final result. This decorator will apply the transformations in the order.
    Then, the final_result function is called, with the result of the transformations passed in."""
    return _join(all_initial_data, raw_data_3)
```

`with_columns` takes in the following parameters (see the docstring for more info)
1. `load_from` -- a list of functions/modules to find the functions to load the DAG from, similar to `@subdag`
2. `columns_to_pass` -- not compatible with `external_inputs`. Dependencies specified from the initial dataframe,
injected in. Not that you must use one of this or
3. `pass_dtaframe_as` -- the name of the parameter to inject the initial dataframe into the subdag.
If this is provided, this must be the only pyspark dataframe dependency in the subdag that is not also another
node (column) in the subdag.
4. `select` -- a list of columns to select from the UDF group. If not specified all will be selected.
5. `dataframe` -- the initial dataframe. If not specified, will default to the only dataframe param
in the decorated function (and error if there are multiple).
6. `namespace` -- the namespace of the nodes generated by this -- will default to the function name that is decorated.


`with_columns` serves to _linearize_ the operation, enabling you to define a DAG, and have all your operations run on a single dataframe,
in topological order. You can thus represent a clean, modular, unit-testable string of transformations while also allowing for
complex sets of feature UDFs.  All of these can rely on data passed from a node or parameter external to the group of functions.

You have two options when presenting the initial dataframe/how to read it. Each corresponds to a `with_columns` parameter. You can use:
1.`columns_to_pass` to constrain the columns that must exist in the initial dataframe, which you refer to in your functions. In the example above, the functions can refer to the three columns `column_1_from_dataframe`, `column_2_from_dataframe`, and `column_3_from_dataframe`, but those cannot be named defined by the subdag.
2. `pass_dataframe_as` to pass the dataframe you're transforming in as a specific parameter name to the subdag. This allows you to handle the extraction -- use this if you want to redefine columns in the dataframe/preserve the same names.

```python
import pandas as pd, pyspark.sql as ps

#map_transforms.py

def colums_1_from_dataframe(input_dataframe: ps.DataFrame) -> ps.Column:
    return input_dataframe.column_1_from_dataframe

def column_2_from_dataframe(input_dataframe: ps.DataFrame) -> ps.Column:
    return input_dataframe.column_2_from_dataframe

def column_3(column_1_from_dataframe: pd.Series) -> pd.Series:
    return _some_transform(column_1_from_dataframe)

def column_4(column_2_from_dataframe: pd.Series) -> pd.Series:
    return _some_other_transform(column_2_from_dataframe)

def column_5(column_3: pd.Series, column_4: pd.Series) -> pd.Series:
    return _yet_another_transform(column_3, column_4)
```

```python
from hamilton.experimental.h_spark import with_columns
import pyspark.sql as ps
import map_transforms # file defined above

@with_columns(
    map_transforms, # Load all the functions we defined above
    pass_dataframe_as="input_dataframe", #the upstream dataframe, referred to by downstream nodes, will have this parametter name
)
def final_result(all_initial_data: ps.DataFrame, raw_data_3: ps.DataFrame) -> ps.DataFrame:
    """Gives the final result. This decorator will apply the transformations in the order.
    Then, the final_result function is called, with the result of the transformations passed in."""
    return _join(all_initial_data, raw_data_3)
```

Approach (2) requires functions that take in pyspark dataframes and return pyspark dataframes or columns for the functions reading directly from the dataframe.
If you want to stay in pandas entirely for the `with_columns` group, you should use approach (1).

There are four flavors of transforms supported.

#### Pandas -> Pandas UDFs
These are functions of series:

```python
from hamilton import htypes

def foo(bar: pd.Series, baz: pd.Series) -> htypes.column[pd.Series, int]:
    return bar + 1
```

The rules are the same as vanilla hamilton -- the parameter name determines the upstream dependencies,
and the function name determines the output column name.

Note that, due to the type-specification requirements of pyspark, these have to return a "typed" (`Annotated[]`) series, specified by `htypes.column`. These are adapted
to form pyspark-friendly [pandas UDFs](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)

#### Python primitives -> python primitives UDFs

These are functions of python primitives:

```python
def foo(bar: int, baz: int) -> int:
    return bar + 1
```

These are adapted to standard [pyspark UDFs](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html).

#### pyspark dataframe -> pyspark columns

These are functions that take in a pyspark dataframe (single) and output a pyspark column.

```python
def foo(bar: ps.DataFrame) -> ps.Column:
    return df["bar"] + 1
```

Note that these have two forms:
1. The dataframe specifies the name of the upstream column -- then you just access the column and return a manipulation
2. The dataframe contains more than one column, in which case you need the `@require(...)` decorator, to specify which column you want to use.

```python
import h_spark


@h_spark.require_columns("bar", "baz")
def foo(bar_baz: ps.DataFrame) -> ps.Column:
   return df["bar"] + 1
```

In this case we are only allowed a single dataframe dependency, and the paraemter name does not matter.
These are an out for when the pyspark computational expression is more convenient than the pandas one,
or it is not possible to express it in pandas.

#### pyspark dataframe -> pyspark dataframe

This is the ultimate power-user case, where you can manipulate the dataframe in any way you want.
Note that this and the column-flavor is an _out_, meaning that its a way to jump back to the pyspark world and not have to break up
your map functions for a windowed aggregation.

Note that you can easily shoot yourself in the foot here, so be careful! This should only be used if
you strongly feel the need to inject a map-like (index-preserving, but not row-wise) operation into the DAG,
and the df -> column flavor is not sufficient (and if you find yourself using this a lot, please reach
out, we'd love to hear your use-case).

This has the exact same rules as the column flavor, except that the return type is a dataframe.

```python
import h_spark


@h_spark.require_columns("bar", "baz")
def foo(df: ps.DataFrame) -> ps.DataFrame:
   return df.withColumn("bar", df["bar"] + 1)
```

Note that this is the column-flavor in which you (not the framework) are responsible for calling `withColumn`.


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

## Why are there so many lines in the visualizations for a with_columns group?

Good question! This is because we are adding two sets of edges:
1. The single dataframe being passed through and updated (immutably, which is why it fits so nicely with Hamilton)
2. The original edges, dependent on columns

We are planning, shortly, to display these edges differently, and ideally allow a visualization mode for these specifically (to show the DAG with (1), (2), or both).
