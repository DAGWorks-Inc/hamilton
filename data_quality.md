# Data Quality

Hamilton has a simple but powerful data quality capability. This enables you to write functions
that have assertion on the output. For example...

```python
import pandas as pd
import numpy as np
from hamilton.function_modifiers import check_output
from hamilton.data_quality.base import DataValidator

@check_output(
    datatype=np.int64,
    data_in_range=(0,100),
    importance=DataValidator.WARN,
)
def some_int_data_between_0_and_100() -> pd.Series:
    pass
```

In the above, we run two assertions:

1. That the series has an np.int64 datatype
2. That every item in the series in between 0 and 100

Furthermore, the workflow does not fail when this dies. Rather, it logs a warning
(more about configuring that later).

## Design

To add data quality validation, we do a simple DAG manipulation. See comments on the 
`BaseDataValidationDecorator` class for how it works.

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
from hamilton.function_modifiers import check_output
from hamilton.data_quality.base import DataValidator

@check_output_custom(AllPrimeValidator(...))
def prime_number_generator(number_of_primes_to_generate: int) -> pd.Series:
    pass
```

## Urgency Levels

TODO

## Handling the results

TODO
