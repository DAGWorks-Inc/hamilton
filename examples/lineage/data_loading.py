from typing import List

import pandas as pd

from hamilton.function_modifiers import extract_columns, tag, tag_outputs


def _sanitize_columns(
    df_columns: List[str],
) -> List[str]:
    """Helper function to sanitize column names.

    :param df_columns: the current column names
    :return: sanitized column names
    """
    return [c.strip().replace("/", "_per_").replace(" ", "_").lower() for c in df_columns]


# list of columns we want to expose for transformations.
columns_to_extract = [
    "passengerid",
    "survived",
    "pclass",
    "name",
    "sex",
    "age",
    "sibsp",
    "parch",
    "ticket",
    "fare",
    "cabin",
    "embarked",
]


@tag_outputs(age={"PII": "true"}, sex={"PII": "true"})
@extract_columns(*columns_to_extract)  # expose columns for feature functions
@tag(
    source="prod.titantic",
    owner="data-engineering",
    importance="production",
    info="https://internal.wikipage.net/",
    target_="titanic_data",
)
def titanic_data(index_col: str, location: str) -> pd.DataFrame:
    """Input data that someone in data engineering has provided for us.

    Here are the features in the data:
        survived - Survival (0 = No; 1 = Yes)
        class - Passenger Class (1 = 1st; 2 = 2nd; 3 = 3rd)
        name - Name
        sex - Sex
        age - Age
        sibsp - Number of Siblings/Spouses Aboard
        parch - Number of Parents/Children Aboard
        ticket - Ticket Number
        fare - Passenger Fare
        cabin - Cabin
        embarked - Port of Embarkation (C = Cherbourg; Q = Queenstown; S = Southampton)
        boat - Lifeboat (if survived)
        body - Body number (if did not survive and body was recovered)

    :param index_col: the column to use as the index
    :param location: the path location of the data
    :return: a dataframe
    """
    df = pd.read_csv(location)  # pretend we changed how to load this.
    df.columns = _sanitize_columns(df.columns)
    df = df.set_index(index_col)
    return df


def target(titanic_data: pd.DataFrame, target_col: str) -> pd.Series:
    """The target column values we want to predict."""
    return titanic_data[target_col]
