from itertools import cycle
from typing import Any, Callable, Iterable, List, Tuple, Union

import numpy as np
import pandas as pd
from scipy.stats import linregress

from hamilton.function_modifiers import extract_fields, tag
from hamilton.htypes import Collect, Parallelizable

Splitter = Callable[[pd.DataFrame], Iterable[tuple[str, pd.DataFrame]]]


def load_data() -> pd.DataFrame:
    """Make some fake data."""
    n_rows = 200
    views = np.linspace(0, 10_000.0, n_rows)
    spend = views * np.sin(np.arange(n_rows))
    _regions = cycle(["Central", "North", "South"])
    regions = [next(_regions) for _ in range(n_rows)]
    _methods = cycle(["Internet", "TV"])
    method = [next(_methods) for _ in range(n_rows)]
    df = (
        pd.DataFrame()
        .assign(Views=views)
        .assign(Spend=spend)
        .assign(Region=regions)
        .assign(Method=method)
    )
    return df


def split_to_groups(
    load_data: pd.DataFrame, funcs: List[Splitter]
) -> Parallelizable[tuple[str, pd.DataFrame]]:
    """Split data into interesting groups."""
    for func in funcs:
        for grp_name, grp in func(load_data):
            yield (grp_name, grp)


@extract_fields(dict(data=pd.DataFrame, group_name=str))
def expander(split_to_groups: tuple[str, pd.DataFrame]) -> dict[str, Any]:
    return {"data": split_to_groups[1], "group_name": split_to_groups[0]}


def average(data: pd.DataFrame) -> float:
    """Average the views."""
    return data.Views.mean()


def model_fit(data: pd.DataFrame, group_name: str) -> Tuple[float, float, float]:
    """Imagine a model fit that doesn't always work."""
    if "Method:TV" in group_name:
        raise Exception("Fake floating point error, e.g.")
    xs = data.Spend.values
    ys = data.Views.values
    res = linregress(xs, ys)
    return res.intercept, res.slope, res.rvalue


@tag(keep_sentinels="true")
def gather_metrics(
    group_name: Union[str, None],
    average: Union[float, None],
    model_fit: Union[Tuple[float, float, float], None],
) -> dict[str, Any]:
    answer = {
        "Name": group_name,
        "Average": average,
        "Intercept": model_fit[0] if model_fit else None,
        "Spend_Coef": model_fit[1] if model_fit else None,
        "Model_Fit": model_fit[2] if model_fit else None,
    }
    return answer


def final_collect(gather_metrics: Collect[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame.from_records(gather_metrics)
    return df
