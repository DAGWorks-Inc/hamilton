"""Basic usage of Hamilton + Ibis

For those coming from SQL or PySpark where Tables / Dataframes are the main 
construct, this will feel intuitive. Each Hamilton function defines a 
meaningful artifact in our dataflow. You can execute expressions in Hamilton functions
or after they are returned by the Driver.

Key Ibis concepts:
- expressions aren't executed until explicitly calling `.to_pandas()` or equivalent.
- use `ibis.expr.types as ir` to type annotate your functions
- `ibis._` or `from ibis import _` can be used in expression to refer to the current table
- .mutate(col1=, col2=, ...) allows to assign new column or overwrite existing one 
"""

from typing import Optional

import ibis
from ibis import _
import ibis.expr.types as ir


def raw_table(raw_data_path: str) -> ir.Table:
    """Load CSV into Table expression and rename columns to snakecase
    
    Columns:
        id
        reason_for_absence
        month_of_absence
        day_of_the_week 
        seasons
        transportation_expense
        distance_from_residence_to_work
        service_time
        age
        work_load_average/day
        hit_target
        disciplinary_failure
        education
        son
        social_drinker
        social_smoker
        pet
        weight
        height
        body_mass_index
        absenteeism_time_in_hours
        has_children
        has_pet
        is_summer_br
    """
    return ibis.read_csv(sources=raw_data_path, table_name="absenteism").rename("snake_case")


def feature_table(raw_table: ir.Table) -> ir.Table:
    """Add feature columns to raw_table"""
    return raw_table.mutate(
        has_children=(ibis.ifelse(_.son > 0, 1, 0)),
        has_pet=ibis.ifelse(_.pet > 0, 1, 0),
        is_summer_brazil=_.month_of_absence.isin([1, 2, 12]).cast(int),
    )
    
    
def age_mean(raw_data: ir.Table) -> ir.Scalar:
    """Average of age"""
    return raw_data.age.mean()

    
def feature_set(
    feature_table: ir.Table,
    feature_selection: list[str],
    condition: Optional[ibis.common.deferred.Deferred] = None,
) -> ir.Table:
    """Select columns based on list of columns"""
    return feature_table[feature_selection].filter(condition)


if __name__ == "__main__":
    from hamilton import driver
    import __main__
    
    dr = driver.Builder().with_modules(__main__).build()
    
    inputs=dict(
        raw_data_path="../data_quality/simple/Absenteeism_at_work.csv",
        feature_selection=[
            "id", "has_children", "has_pet", "is_summer_brazil",
            "service_time", "seasons", "disciplinary_failure", "absenteeism_time_in_hours",
        ],
        condition=ibis.ifelse(_.has_pet == 1, True, False)
    )
    
    final_vars = ["feature_set"]
    
    res = dr.execute(final_vars, inputs=inputs)
    
    df = res[final_vars[0]].to_pandas()
    print(df.head())
    print(df.shape)
