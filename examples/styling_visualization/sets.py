import pandas as pd

from hamilton.function_modifiers import tag

# --- creating the dataset


@tag(owner="data-science", importance="production", artifact="training_set")
def training_set_v1(
    pclass: pd.Series,
    age: pd.Series,
    fare: pd.Series,
    cabin_category: pd.Series,
    sex_category: pd.Series,
    embarked_category: pd.Series,
    family: pd.Series,
) -> pd.DataFrame:
    """Creates the dataset -- this is one way to do it. Explicitly make a function.

    :param pclass:
    :param age:
    :param fare:
    :param cabin_category:
    :param sex_category:
    :param embarked_category:
    :param family:
    :return: a data set to use for model building.
    """
    df = pd.DataFrame(
        {
            "pclass": pclass,
            "age": age,
            "fare": fare,
            "cabin_category": cabin_category,
            "sex_category": sex_category,
            "embarked_category": embarked_category,
            "family": family,
        }
    )
    df.fillna(0, inplace=True)
    return df
