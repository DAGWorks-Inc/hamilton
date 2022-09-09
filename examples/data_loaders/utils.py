from typing import Callable

import numpy as np


def generate_random_walk_time_series(
    num_datapoints: int,
    start_value: float,
    step_mean: float,
    step_stddev: float,
    cumulative: bool = True,
    min_value: float = None,
    max_value: float = None,
    apply: Callable = lambda x: x,
) -> np.array:
    random_nums = np.random.normal(step_mean, step_stddev, num_datapoints)
    out = []
    curr = start_value
    for random_num in random_nums:
        if cumulative:
            curr += random_num
        else:
            curr = random_num
        if min_value is not None:
            curr = max(curr, min_value)
        if max_value is not None:
            curr = min(curr, max_value)
        out.append(curr)
    return [apply(item) for item in out]
