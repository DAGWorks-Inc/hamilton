from typing import Any, Dict

import pydantic
from hamilton_sdk.tracking import stats


@stats.compute_stats.register
def compute_stats_pydantic(
    result: pydantic.BaseModel, node_name: str, node_tags: dict
) -> Dict[str, Any]:
    if hasattr(result, "dump_model"):
        llm_result = result.dump_model()
    else:
        llm_result = result.dict()
    return {
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(result)),
            "value": llm_result,
        },
        "observability_schema_version": "0.0.2",
    }
