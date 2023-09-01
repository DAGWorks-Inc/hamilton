import random

import pandas as pd
from pandas import DataFrame


# This is a simple placeholder for a model. You'll likely want to adjust the typing to make it
# work for your case, or use a pretrained model.
class Model:
    def predict(self, features: DataFrame) -> pd.Series:
        return pd.Series([random.random() for item in features.iterrows()])


def model() -> Model:
    return Model()


def features(
    time_since_last_login: pd.Series,
    is_male: pd.Series,
    is_female: pd.Series,
    is_high_roller: pd.Series,
    age_normalized: pd.Series,
) -> pd.DataFrame:
    """Aggregate all features into a single dataframe.
    :param time_since_last_login: Feature the model cares about
    :param is_male: Feature the model cares about
    :param is_female: Feature the model cares about
    :param is_high_roller: Feature the model cares about
    :param age_normalized: Feature the model cares about
    :return: All features concatenated into a single dataframe
    """
    return pd.DataFrame(locals())


def predictions(features: pd.DataFrame, model: Model) -> pd.Series:
    """Simple call to your model over your features."""
    return model.predict(features)
