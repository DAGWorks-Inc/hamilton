from typing import Any, Dict

import numpy as np
import pandas as pd
from hamilton_sdk.tracking import pandas_stats, stats

"""Module that houses functions to compute statistics on numpy objects
Notes:
 - we should assume numpy v1.0+ so that we have a string type
"""


@stats.compute_stats.register
def compute_stats_numpy(result: np.ndarray, node_name: str, node_tags: dict) -> Dict[str, Any]:
    try:
        df = pd.DataFrame(result)  # hack - reuse pandas stuff
    except ValueError:
        return {
            "observability_type": "unsupported",
            "observability_value": {
                "unsupported_type": str(type(result)) + f" with dimensions {result.shape}",
                "action": "reach out to the DAGWorks team to add support for this type.",
            },
            "observability_schema_version": "0.0.1",
        }
    return {
        "observability_type": "dagworks_describe",
        "observability_value": pandas_stats._compute_stats(df),
        "observability_schema_version": "0.0.3",
    }
