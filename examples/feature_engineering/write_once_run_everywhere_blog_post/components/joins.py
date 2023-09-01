import pandas as pd

from hamilton.function_modifiers import config


@config.when(mode="batch")
def last_logged_in__batch(client_id: pd.Series, client_login_data: pd.DataFrame) -> pd.Series:
    return pd.merge(client_id, client_login_data, left_on="client_id", right_index=True)[
        "last_logged_in"
    ]
