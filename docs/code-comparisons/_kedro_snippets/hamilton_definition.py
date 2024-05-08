# dataflow.py
import pandas as pd

def _is_true(x: pd.Series) -> pd.Series:
    return x == "t"

def companies_preprocessed(companies: pd.DataFrame) -> pd.DataFrame:
    """Companies with added column `iata_approved`"""
    companies["iata_approved"] = _is_true(companies["iata_approved"])
    return companies

def shuttles_preprocessed(shuttles: pd.DataFrame) -> pd.DataFrame:
    """Shuttles with added columns `d_check_complete`
    and `moon_clearance_complete`."""
    shuttles["d_check_complete"] = _is_true(
        shuttles["d_check_complete"]
    )
    shuttles["moon_clearance_complete"] = _is_true(
        shuttles["moon_clearance_complete"]
    )
    return shuttles

def model_input_table(
    shuttles_preprocessed: pd.DataFrame,
    companies_preprocessed: pd.DataFrame,
) -> pd.DataFrame:
    """Table containing shuttles and companies data."""
    shuttles_preprocessed = shuttles_preprocessed.drop("id", axis=1)
    model_input_table = shuttles_preprocessed.merge(
        companies_preprocessed, left_on="company_id", right_on="id"
    )
    model_input_table = model_input_table.dropna()
    return model_input_table