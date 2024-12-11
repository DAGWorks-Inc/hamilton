from typing import Any

import features
import inference
import pandas as pd
import train

from hamilton.function_modifiers import configuration, extract_fields, source, subdag


@extract_fields({"fit_model": Any, "training_prediction": pd.DataFrame})
@subdag(
    features,
    train,
    inference,
    inputs={
        "path": source("path"),
        "model_params": source("model_params"),
    },
    config={
        "model": configuration("train_model_type"),  # not strictly required but allows us to remap.
    },
)
def trained_pipeline(fit_model: Any, predicted_data: pd.DataFrame) -> dict:
    return {"fit_model": fit_model, "training_prediction": predicted_data}


@subdag(
    features,
    inference,
    inputs={
        "path": source("predict_path"),
        "fit_model": source("fit_model"),
    },
)
def predicted_data(predicted_data: pd.DataFrame) -> pd.DataFrame:
    return predicted_data
