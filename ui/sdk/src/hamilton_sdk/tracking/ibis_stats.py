from typing import Any, Dict

from hamilton_sdk.tracking import stats
from ibis.expr.datatypes import core

# import ibis.expr.types as ir
from ibis.expr.types import relations

"""Module that houses functions to introspect an Ibis Table. We don't have expression support yet.
"""

base_data_type_mapping_dict = {
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


def base_data_type_mapping(data_type: core.DataType) -> str:
    """Returns the base data type of the column.
    This uses the internal is_* type methods to determine the base data type.
    """
    return "unhandled"  # TODO: implement this


base_schema = {
    # we can't get all of these about an ibis dataframe
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


def _introspect(table: relations.Table) -> Dict[str, Any]:
    """Introspect a PySpark dataframe and return a dictionary of statistics.

    :param df: PySpark dataframe to introspect.
    :return: Dictionary of column to metadata about it.
    """
    # table.
    fields = table.schema().items()
    column_to_metadata = []
    for idx, (field_name, field_type) in enumerate(fields):
        values = base_schema.copy()
        values.update(
            {
                "name": field_name,
                "pos": idx,
                "data_type": str(field_type),
                "base_data_type": base_data_type_mapping(field_type),
                "nullable": field_type.nullable,
            }
        )
        column_to_metadata.append(values)
    return {
        "columns": column_to_metadata,
    }


@stats.compute_stats.register
def compute_stats_ibis_table(
    result: relations.Table, node_name: str, node_tags: dict
) -> Dict[str, Any]:
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
