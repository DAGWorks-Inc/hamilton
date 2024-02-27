"""Basic usage of Hamilton + Ibis with column level lineage

This approach uses Hamilton functions to define column level expressions for better lineage.
A join step with `.mutate()` is required to assemble columns into a single table. The code 
is not much more verbose than the "table approach" and it provides better documentation and
modularity. Again, you can execute expressions in Hamilton functions
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

from hamilton.function_modifiers import extract_columns
from hamilton.plugins import ibis_extensions


RAW_COLUMNS = [
    'id', 'reason_for_absence', 'month_of_absence', 'day_of_the_week',
    'age', 'disciplinary_failure', 'son', "seasons",
    'pet', 'absenteeism_time_in_hours'
]


@extract_columns(*RAW_COLUMNS)
def raw_data(raw_data_path: str) -> ir.Table:
    """Load CSV into Table expression and rename columns to snakecase"""
    return ibis.read_csv(sources=raw_data_path, table_name="absenteism").rename("snake_case")


def has_children(son: ir.Column) -> ir.Column:
    """Single variable that says whether someone has any children or not."""
    return ibis.ifelse(son > 0, 1, 0).cast(bool)


def has_pet(pet: ir.Column) -> ir.Column:
    """Single variable that says whether someone has any pets or not."""
    return ibis.ifelse(pet > 0, 1, 0).cast(bool)


# narrows the return type from `ir.Column` to `ir.BooleanColumn`
def is_summer_brazil(month_of_absence: ir.Column) -> ir.BooleanColumn:
    """Boolean if it is summer in Brazil this month"""
    return month_of_absence.isin([1, 2, 12])


# returns a scalar, not a ir.Column
def age_mean(age: ir.Column) -> ir.Scalar:
    """Average of age"""
    return age.mean()


# depends on a column and a scalar
def age_normalized(age: ir.Column, age_mean: ir.Scalar) -> ir.Column:
    """Zero mean unit variance value of age"""
    return ((age - age_mean) / age.std())


def feature_table(
    id: ir.Column,
    service_time: ir.Column,
    disciplinary_failure: ir.Column,
    seasons: ir.Column,
    has_children: ir.Column,
    has_pet: ir.Column,
    is_summer_brazil: ir.Column,
    age_normalized: ir.Column,
    absenteeism_time_in_hours: ir.Column,
) -> ir.Table:
    return ibis.table().mutate(
        id=id,
        seasons=seasons,
        service_time=service_time,
        disciplinary_failure=disciplinary_failure,
        has_children=has_children,
        has_pet=has_pet,
        is_summer_brazil=is_summer_brazil,
        age_normalized=age_normalized,
        absenteeism_time_in_hours=absenteeism_time_in_hours,
    )
    

def feature_set(
    feature_table: ir.Table,
    feature_selection: list[str],
    condition: Optional[ibis.common.deferred.Deferred] = None,
) -> ir.Table:
    """Select columns based on list of columns
    
    NOTE trying to select a column that doesn't exist won't
    throw an exception
    """
    return feature_table[feature_selection].filter(condition)


if __name__ == "__main__":
    from hamilton import driver
    import __main__
    
    dr = driver.Builder().with_modules(__main__).build()
    inputs=dict(
        raw_data_path="../data_quality/simple/Absenteeism_at_work.csv",
        feature_selection=[
            "id", "has_children", "has_pet", "is_summer_brazil", "age_normalized",
            "service_time", "seasons", "disciplinary_failure",
        ],
        condition=ibis.ifelse(_.has_pet == 1, True, False)
    )
    
    final_vars = ["feature_set"]
    
    res = dr.execute(final_vars, inputs=inputs)
    
    df = res["feature_set"].to_pandas()
    print(df.head())
    print(df.shape)
