import pandas as pd

from hamilton.function_modifiers import does, extract_columns


def _sum_multiply(param0: int, param1: int, param2: int) -> pd.DataFrame:
    return pd.DataFrame([{"param0a": param0, "param1b": param1, "param2c": param2}])


@extract_columns("param1b")
@does(_sum_multiply)
def to_modify(param0: int, param1: int, param2: int = 2) -> pd.DataFrame:
    """This sums the inputs it gets..."""
    pass
