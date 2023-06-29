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
    return cleaned_df.head(20)


@extract_fields(
    dict(
        ids=list[str],
        titles=list[str],
        text_contents=list[str],
    )
)
def dataset_rows(clean_dataset: pd.DataFrame) -> dict:
    """Convert dataframe to dict of lists"""
    df_as_dict = clean_dataset.to_dict(orient="list")
    return dict(
        ids=df_as_dict["id"], titles=df_as_dict["title"], text_contents=df_as_dict["context"]
    )
