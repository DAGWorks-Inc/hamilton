from inspect import cleandoc
from io import StringIO

import pandas as pd
from pandas import DataFrame

from my_wrapper import TaxCalculator


def read_table(table: str, delimiter="|") -> DataFrame:
    """
    Read table from string and return pandas DataFrame.
    """
    df = pd.read_table(StringIO(cleandoc(table)), delimiter=delimiter)
    df = df.loc[:, ~df.columns.str.match("Unnamed")]
    df.columns = df.columns.str.strip()
    return df


input = read_table(
    """
    | Name     | Income | Children |
    | John     | 75600  | 2        |
    | Bob      | 34000  | 1        |
    | Chloe    | 111500 | 3        |
    | Thomas   | 234546 | 1        |
    | Ellis    | 144865 | 2        |
    | Deane    | 138500 | 4        |
    | Mariella | 69412  | 5        |
    | Carlos   | 65535  | 0        |
    | Toney    | 43642  | 3        |
    | Ramiro   | 117850 | 2        |
    """
)


# Visualize the DAG
TaxCalculator.visualize()

# Calculate the taxes
output = TaxCalculator.calculate(input)
print(output.to_string())