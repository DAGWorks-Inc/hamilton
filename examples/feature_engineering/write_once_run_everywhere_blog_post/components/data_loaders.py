import pandas as pd
from components import utils

from hamilton.function_modifiers import config, extract_columns


@config.when(mode="batch")
@extract_columns("budget", "age", "gender", "client_id")
def survey_results(survey_results_table: str, survey_results_db: str) -> pd.DataFrame:
    """Map operation to explode survey results to all fields
    Data comes in JSON, we've grouped it into a series.
    """
    return utils.query_table(table=survey_results_table, db=survey_results_db)


@config.when(mode="online")
@extract_columns(
    "budget",
    "age",
    "gender",
)
def survey_results__online(client_id: int) -> pd.DataFrame:
    """Map operation to explode survey results to all fields
    Data comes in JSON, we've grouped it into a series.
    """
    return utils.query_survey_results(client_id=client_id)


@config.when(mode="batch")
def client_login_data__batch(client_login_db: str, client_login_table: str) -> pd.DataFrame:
    """Load the client login data"""
    return utils.query_table(table=client_login_table, db=client_login_db)


@config.when(mode="online")
def last_logged_in__online(client_id: int) -> pd.Series:
    """Query a service for the client login data"""
    return utils.query_login_data(client_id=client_id)["last_logged_in"]
