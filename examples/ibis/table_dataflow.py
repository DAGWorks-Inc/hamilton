from typing import Optional

import ibis
import ibis.expr.types as ir

from hamilton.function_modifiers import check_output  # noqa: F401
from hamilton.plugins import ibis_extensions  # noqa: F401


def raw_table(raw_data_path: str) -> ir.Table:
    """Load CSV from `raw_data_path` into a Table expression
    and format column names to snakecase
    """
    return ibis.read_csv(sources=raw_data_path, table_name="absenteism").rename("snake_case")


# @check_output(
#     schema=ibis.schema(
#         [("has_children", "int"), ("has_pet", "bool")]
#     )
# )
def feature_table(raw_table: ir.Table) -> ir.Table:
    """Add to `raw_table` the feature columns `has_children`
    `has_pet`, and `is_summer_brazil`
    """
    return raw_table.mutate(
        has_children=(ibis.ifelse(ibis._.son > 0, True, False)),
        has_pet=ibis.ifelse(ibis._.pet > 0, True, False),
        is_summer_brazil=ibis._.month_of_absence.isin([1, 2, 12]),
    )


def feature_set(
    feature_table: ir.Table,
    feature_selection: list[str],
    condition: Optional[ibis.common.deferred.Deferred] = None,
) -> ir.Table:
    """Select feature columns and filter rows"""
    return feature_table[feature_selection].filter(condition)


if __name__ == "__main__":
    import __main__

    from hamilton import driver

    dr = driver.Builder().with_modules(__main__).build()

    inputs = dict(
        raw_data_path="../data_quality/simple/Absenteeism_at_work.csv",
        feature_selection=[
            "id",
            "has_children",
            "has_pet",
            "is_summer_brazil",
            "service_time",
            "seasons",
            "disciplinary_failure",
            "absenteeism_time_in_hours",
        ],
        condition=ibis.ifelse(ibis._.has_pet == 1, True, False),
    )
    dr.display_all_functions("schema.png", show_schema=True)

    res = dr.execute(["feature_set"], inputs=inputs)
    breakpoint()
    df = res["feature_set"].to_pandas()
    print(df.head())
    print(df.shape)
