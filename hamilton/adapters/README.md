# Data Loaders

Hamilton comes with tooling that allows you to easily load data from external sources.
While you *can* load data from within your functions (after all, Hamilton places no
restrictions on what you do with your functions), using Hamilton's data loading
infrastructure provides a myriad of benefits:

1. Less repeated code
    The logic for all data loading is centralized by the framework, and can be reused.
2. Cleaner looking code
    The code to load data will not be combined with the code to process it.
3. Configuration-driven data loading
    Hamilton's data loaders consume configuration parameters that allow for sensible defaults. This gives you fine-grained control over the way your data loads and allows for setting paramters once, globally.
4. Shared connection pools
    Hamilton's data loaders have the capability of registering/sharing db connections, enabling you to write multiple loaders without handling connection-related issues yourself.
5. Highly customizable compponents
    Hamilton's data loaders assume commonalities, but

## Initial Examples

Alright, let's jump into a few examples that highlight the points above:

### Loading from a CSV

With the following example, we load a pandas dataframe from a local path. Note some things:

1. The CSVLoader is instantiated with no params -- all of these will be configurable.
2. We pass path/include_columns to the CSVLoader, this will get injected into its 'load' section.
3. There are no other configuration elements needed, so this is all we need to run this.
```python
import pandas as pd
from hamilton.function_modifiers.data_loaders import CSVLoader, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns
@extract_columns("col1", "col2")
@load_from(
    CSVLoader(),
    path=config("my_data.csv_path"),
    include_columns=["col1", "col2"],
    separator=","
)
def loaded_data(df: pd.DataFrame)  -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df
```

### Loading raw data from S3

```python
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3RawData, load_from
from hamilton.function_modifiers.dependencies import config
from io import BytesIO
from hamilton.function_modifiers import extract_columns
@extract_columns("col1", "col2")
@load_from(
    S3RawData(format="csv"),
    # S3LoaderToDataFrame(format="pickle")
    bucket=config("loaded_data.s3.bucket"),
    key=config("loaded_data.s3.key"),
)
def loaded_data(bytes_io: BytesIO)  -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df
```

### Loading a dataframe from S3

#### Configuring bucket/key
```python
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3LoaderToDataFrame, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns
@extract_columns("col1", "col2")
@load_from(
    S3LoaderToDataFrame(format="csv"),
    # S3LoaderToDataFrame(format="pickle")
    bucket=config("loaded_data.s3.bucket"),
    key=config("loaded_data.s3.key"),
)
def loaded_data(df: pd.DataFrame)  -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df
```

### Running a SQL query against a postgres database

You can run a simple SQL query if you want. Note that this requires a configuration parameter set called `postgres` with the follwoing keys:

1. `host`
2. `port`
3. `user`
4. `password`
5. `database`

```python

Note that this does not make use of multiple connections, and instead uses a default connection.
```python
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3LoaderToDataFrame, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns
@extract_columns("col1", "col2")
@load_from(
    PostgresSQL(),
    query="SELECT col_1 as col1, col_2 as col2 FROM my_table WHERE condition_column = 1",
)
def loaded_data(df: pd.DataFrame)  -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df

# This uses a shared connection. Defaults to the standard connection
# Use this if you want to
dr = driver.Driver({
    'postgres.default_connection' : {
        'host': 'YOUR_HOST',
        'port': 5432,  # probably
        'user': 'YOUR_USERNAME',
        'password': os.environ['POSTGRES_PASSWORD'], # Or some other secure way of loading it
        'database' : 'YOUR_DATABASE'
    }
}, data_loaders)
dr.execute(['col3, 'col4'])
```

In this case you'd add a dict called `postgres.secondary_connection` with the following to the config:

1. `host`
2. `port`
3. `user`
4. `password`
5. `database`

```python
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3LoaderToDataFrame, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns

# Option 1:
@extract_columns("col3", "col4")
@load_from(
    PostgresSQL(),
    query="SELECT col_3 as col3, col_4 as col4 FROM my_table WHERE condition_column = 1",
    connection="secondary_connection" # This
)
def loaded_data_2(df: pd.DataFrame)  -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df

# Option 2:

@load_from.sql(
    query="SELECT col_3 as col3, col_4 as col4 FROM my_table WHERE condition_column = 1",
    connection="secondary_connection" # This
)


# This uses a namespaced connection
dr = driver.Driver({
    'postgres.secondary_connection' : {
        'host': 'YOUR_HOST',
        'port': 5432,  # probably
        'user': 'YOUR_USERNAME',
        'password': os.environ['POSTGRES_PASSWORD'], # Or some other secure way of loading it
        'database' : 'YOUR_DATABASE'
    }
}, data_loaders)
dr.execute(['col3', 'col4'])
```
If you prefer not to handle configurations like this, you can just pass it in directly.
This is nice for ad-hoc work, but the connection won't be shared/cached.

Furthermore, you have less control over the source of the inputs.

```python
import os
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3LoaderToDataFrame, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns

@load_from.sql(
    connection="default_connection"
)
def loaded_data_2(condition_number: int) -> Table:
    return "SELECT col_3 as col3, col_4 as col4 FROM my_table WHERE condition_column = {{condition_number}}"

@extract_columns("col3", "col4")
@load_from(
    PostgresSQL(),
    query=value("query"),
    connection={
        'host': 'YOUR_HOST',
        'port': 5432,  # probably
        'user': 'YOUR_USERNAME',
        'password': os.environ['POSTGRES_PASSWORD'] # Or some other secure way of loading it
        'database' : 'YOUR_DATABASE'
    }
)
def loaded_data_2(df: pd.DataFrame) -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df

# No config needed here:
dr = driver.Driver({}, data_loaders)
dr.execute({}['col3', 'col4'])
```

If you forget any of the configurations above, we will complain loudly within the loader so you know which ones to add.


### Running a parameterized query against a postgres database

You can also utilize jinja templates -- this effectively makes the SQL query now accept inputs/configs. E.G. other nodes, configs, inputs, etc...

Just remember to pass them into the driver!

In this case we're passing them in as inputs. Note that the SQL statements can be as arbitrary complex as you want,
and they can even refer to upstream functions for their parameters, in case you want to get really fancy.

```python
import os
import pandas as pd
from hamilton.function_modifiers.data_loaders import S3LoaderToDataFrame, load_from
from hamilton.function_modifiers.dependencies import config
from hamilton.function_modifiers import extract_columns


@extract_columns("col3", "col4")
@load_from(
    PostgresSQL(),
    query="SELECT col_3 as col3, col_4 as col4 FROM my_table WHERE condition_column = {{filter_condition}}",
)
def loaded_data_2(df: pd.DataFrame) -> pd.DataFrame:
    """Data is loaded above, passed in, and you have the change to modify it!"""
    return df

dr = driver.Driver({
    "postgres.default_connection" : {...}
}, data_loaders)
dr.execute(['col3', 'col4'], {'filter_condition' : 1})
```


### Loading a dataframe from a parquet file

### Loading from the OpenAI API

### Loading from BigQuery with an SQL query

###

## SQL

## Extending

## How this works
