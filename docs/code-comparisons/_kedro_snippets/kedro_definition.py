# nodes.py
import pandas as pd

def _is_true(x: pd.Series) -> pd.Series:
    return x == "t"

def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for companies."""
    companies["iata_approved"] = _is_true(companies["iata_approved"])
    return companies

def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for shuttles."""
    shuttles["d_check_complete"] = _is_true(
        shuttles["d_check_complete"]
    )
    shuttles["moon_clearance_complete"] = _is_true(
        shuttles["moon_clearance_complete"]
    )
    return shuttles

def create_model_input_table(
    shuttles: pd.DataFrame, companies: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a model input table."""
    shuttles = shuttles.drop("id", axis=1)
    model_input_table = shuttles.merge(
        companies, left_on="company_id", right_on="id"
    )
    model_input_table = model_input_table.dropna()
    return model_input_table