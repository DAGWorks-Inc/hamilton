import pandas as pd
from datasets import load_dataset

from hamilton.function_modifiers import extract_fields


def squad_dataset() -> pd.DataFrame:
    """Load the SQuaD dataset using the HuggingFace Dataset library"""
    dataset = load_dataset("squad", split="train")
    return dataset.to_pandas()


def clean_dataset(squad_dataset: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicates and drop unecessary columns"""
    cleaned_df = squad_dataset.drop_duplicates(subset=["context"], keep="first")
    cleaned_df = cleaned_df[["id", "title", "context"]]
    return cleaned_df


def filtered_dataset(clean_dataset: pd.DataFrame, n_rows_to_keep: int = 20) -> pd.DataFrame:
    if n_rows_to_keep is None:
        return clean_dataset

    return clean_dataset.head(n_rows_to_keep)


@extract_fields(
    dict(
        ids=list[str],
        titles=list[str],
        text_contents=list[str],
    )
)
def dataset_rows(filtered_dataset: pd.DataFrame) -> dict:
    """Convert dataframe to dict of lists"""
    df_as_dict = filtered_dataset.to_dict(orient="list")
    return dict(
        ids=df_as_dict["id"], titles=df_as_dict["title"], text_contents=df_as_dict["context"]
    )
