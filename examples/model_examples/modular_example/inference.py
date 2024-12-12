from typing import Any

import pandas as pd


def predicted_data(transformed_data: pd.DataFrame, fit_model: Any) -> pd.DataFrame:
    return fit_model.predict(transformed_data)
