from typing import Any, Dict

import pyspark.sql as ps
from hamilton_sdk.tracking import stats

"""Module that houses functions to introspect a PySpark dataframe.
"""
# this is a mapping used in the Backend/UI.
# we should probably move this to a shared location.
base_data_type_mapping = {
    "timestamp": "datetime",
    "date": "datetime",
    "string": "str",
    "integer": "numeric",
    "double": "numeric",
    "float": "numeric",
    "boolean": "boolean",
    "long": "numeric",
    "short": "numeric",
}

base_schema = {
    # we can't get all of these about a pyspark dataframe
    "base_data_type": None,
    # 'count': 0,
    "data_type": None,
    # 'histogram': {},
    # 'max': 0,
    # 'mean': 0,
    # 'min': 0,
    # 'missing': 0,
    "name": None,
    "pos": None,
    # 'quantiles': {},
    # 'std': 0,
    # 'zeros': 0
}


def _introspect(df: ps.DataFrame) -> Dict[str, Any]:
    """Introspect a PySpark dataframe and return a dictionary of statistics.

    :param df: PySpark dataframe to introspect.
    :return: Dictionary of column to metadata about it.
    """
    fields = df.schema.jsonValue()["fields"]
    column_to_metadata = []
    for idx, field in enumerate(fields):
        values = base_schema.copy()
        values.update(
            {
                "name": field["name"],
                "pos": idx,
                "data_type": field["type"],
                "base_data_type": base_data_type_mapping.get(field["type"], "unhandled"),
                "nullable": field["nullable"],
            }
        )
        column_to_metadata.append(values)
    cost_explain = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "cost")
    extended_explain = df._sc._jvm.PythonSQLUtils.explainString(
        df._jdf.queryExecution(), "extended"
    )
    return {
        "columns": column_to_metadata,
        "cost_explain": cost_explain,
        "extended_explain": extended_explain,
    }


@stats.compute_stats.register
def compute_stats_psdf(result: ps.DataFrame, node_name: str, node_tags: dict) -> Dict[str, Any]:
    # TODO: create custom type instead of dict for UI
    o_value = _introspect(result)
    return {
        "observability_type": "dict",
        "observability_value": {
            "type": str(type(result)),
            "value": o_value,
        },
        "observability_schema_version": "0.0.2",
    }


if __name__ == "__main__":
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": ["a", "b", "c", "d", "e"],
            "c": [True, False, True, False, True],
            "d": [1.0, 2.0, 3.0, 4.0, 5.0],
            "e": pd.Categorical(["a", "b", "c", "d", "e"]),
            "f": pd.Series(["a", "b", "c", "d", "e"], dtype="string"),
            "g": pd.Series(["a", "b", "c", "d", "e"], dtype="object"),
            "h": pd.Series(
                ["20221231", None, "20221231", "20221231", "20221231"], dtype="datetime64[ns]"
            ),
            "i": pd.Series([None, None, None, None, None], name="a", dtype=np.float64),
            "j": pd.Series(name="a", data=pd.date_range("20230101", "20230105")),
        }
    )
    spark = ps.SparkSession.builder.master("local[1]").getOrCreate()
    psdf = spark.createDataFrame(df)
    import pprint

    res = compute_stats_psdf(psdf, "df", {})
    pprint.pprint(res)
