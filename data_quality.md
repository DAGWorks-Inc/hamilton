# Data Quality

Hamilton has a simple but powerful data quality capability. This enables you to write functions
that have assertion on the output. For example...

```python
import pandas as pd
import numpy as np
from hamilton.function_modifiers import check_output

@check_output(
    datatype=np.int64,
    data_in_range=(0,100),
    importance="warn",
)
def some_int_data_between_0_and_100() -> pd.Series:
    pass
```

In the above, we run two assertions:

1. That the series has an np.int64 datatype
2. That every item in the series in between 0 and 100

Furthermore, the workflow does not fail when this dies. Rather, it logs a warning
More about configuring that later, but you can see its specified in the `importance` parameter above.

## Design

To add data quality validation, we run an additional computational step in your workflow after function calculation.
See comments on the `BaseDataValidationDecorator` class for how it works.

## Default Validators

The available default validators are listed in the variable `AVAILABLE_DEFAULT_VALIDATORS`
in `default_validators.py`. To add more, please implement the class in that file then add to the list.
There is a test that ensures that everything is added to that list.

## Custom Validators

To add a custom validator, you need to implement the class `DataValidator`. You can then use the
`@check_output_custom` decorator to run it on a function. For example:

```python
import pandas as pd
import numpy as np

@check_output_custom(AllPrimeValidator(...))
def prime_number_generator(number_of_primes_to_generate: int) -> pd.Series:
    pass
```

## Urgency Levels

Currently there are two available urgency level:

1. "warn"
2. "fail"

They do exactly as you'd expect. "warn" logs the failure to the terminal and continues on. "fail"
raises an exception in the final node.

Limitations/future work are as follows:

1. Currently the actions are hardcoded. In the future, we will be considering adding
special actions for each level that one can customize...
2. One can only disable data quality checks by commenting out the decorator. We intend to allow node-specific overrides.
3. Currently the data quality results apply to every output of that function. E.G. if it runs `extract_columns`
it executes on every column that's extracted.

## Handling the results

We utilize tags to index nodes that represent data quality. All data-quality related tags start with the
prefix `hamilton.data_quality`. Currently there are two:

1. `hamilton.data_quality.contains_dq_results` -- this is a boolean that tells
whether a node outputs a data quality results. These are nodes that get injected when
a node is decorated, and can be queried by the end user.
2. `hamilton.data_quality.source_node` -- this contains the name of the source_node
the data to which the data quality points.

Note that these tags will not be present if the node is not related to data quality --
don't assume they're in every node.

To query one can simply filter for all the nodes that contain these tags and access the results!
