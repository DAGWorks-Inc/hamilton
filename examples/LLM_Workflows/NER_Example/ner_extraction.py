import lancedb
import numpy as np
import pandas as pd
import torch
from datasets import load_dataset
from sentence_transformers import SentenceTransformer
from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline
from transformers.pipelines import base


def medium_articles() -> pd.DataFrame:
    # load the dataset and convert to pandas dataframe
    df = load_dataset(
        "fabiochiu/medium-articles", data_files="medium_articles.csv", split="train"
    ).to_pandas()
    return df


def sampled_articles(medium_articles: pd.DataFrame) -> pd.DataFrame:
    df = medium_articles.dropna().sample(20000, random_state=32)
    # select first 1000 characters
    df["text"] = df["text"].str[:1000]
    # join article title and the text
    df["title_text"] = df["title"] + ". " + df["text"]
    return df


def device() -> int:
    return torch.cuda.current_device() if torch.cuda.is_available() else None


def model_id() -> str:
    # To extract named entities, we will use a NER model finetuned on a BERT-base model.
    # The model can be loaded from the HuggingFace model hub
    return "dslim/bert-base-NER"


def tokenizer(model_id: str) -> AutoTokenizer:
    """load the tokenizer from huggingface"""
    print("Loading the tokenizer")
    return AutoTokenizer.from_pretrained(model_id)


def model(model_id: str) -> object:
    """load the NER model from huggingface"""
    print("Loading the model")
    return AutoModelForTokenClassification.from_pretrained(model_id)


# load the tokenizer and model into a NER pipeline
def ner_pipeline(model: object, tokenizer: AutoTokenizer, device: int) -> base.Pipeline:
    print("Loading the ner_pipeline")
    return pipeline(
        "ner", model=model, tokenizer=tokenizer, aggregation_strategy="max", device=device
    )


def retriever(device: int) -> SentenceTransformer:
    """A retriever model is used to embed passages (article title + first 1000 characters) and queries. It creates embeddings such that queries and passages with similar meanings are close in the vector space. We will use a sentence-transformer model as our retriever. The model can be loaded as follows:"""
    print("Loading the retriever model")
    return SentenceTransformer("flax-sentence-embeddings/all_datasets_v3_mpnet-base", device=device)


def db() -> lancedb.DBConnection:
    return lancedb.connect("./.lancedb")


def batch_size() -> int:
    # we will use batches of 64
    return 64


from tqdm.auto import tqdm


def _extract_named_entities(text_batch, ner_pipeline):
    # extract named entities using the NER pipeline
    extracted_batch = ner_pipeline(text_batch)
    entities = []
    # loop through the results and only select the entity names
    for text in extracted_batch:
        ne = [entity["word"] for entity in text]
        entities.append(ne)
    return entities


def lancedb_data(
    sampled_articles: pd.DataFrame,
    retriever: SentenceTransformer,
    batch_size: int,
    ner_pipeline: base.Pipeline,
) -> list[dict]:
    data = []

    for i in tqdm(range(0, len(sampled_articles), batch_size)):
        # find end of batch
        i_end = min(i + batch_size, len(sampled_articles))
        # extract batch
        batch = sampled_articles.iloc[i:i_end].copy()
        # generate embeddings for batch
        emb = retriever.encode(batch["title_text"].tolist()).tolist()
        # extract named entities from the batch
        entities = _extract_named_entities(batch["title_text"].tolist(), ner_pipeline)
        # remove duplicate entities from each record
        batch["named_entities"] = [list(set(entity)) for entity in entities]
        batch = batch.drop("title_text", axis=1)
        # get metadata
        meta = batch.to_dict(orient="records")
        # create unique IDs
        ids = [f"{idx}" for idx in range(i, i_end)]
        # add all to upsert list
        to_upsert = list(zip(ids, emb, meta, batch["named_entities"]))
        for id, emb, meta, entity in to_upsert:
            temp = {}

            temp["vector"] = np.array(emb)
            temp["metadata"] = meta
            temp["named_entities"] = entity
            data.append(temp)
    return data


def load_into_lancedb(
    lancedb_data: list[dict],
    db: lancedb.DBConnection,
    table_name: str,
) -> int:
    try:
        db.create_table(table_name, lancedb_data)
    except ValueError:
        tbl = db.open_table(table_name)
        tbl.add(lancedb_data)
    return len(lancedb_data)


def lancedb_table(db: lancedb.DBConnection, table_name: str = "tw") -> lancedb.table.Table:
    tbl = db.open_table(table_name)
    return tbl


def search_lancedb(
    query: str,
    ner_pipeline: base.Pipeline,
    retriever: SentenceTransformer,
    lancedb_table: lancedb.table.Table,
) -> dict:
    # extract named entities from the query
    ne = _extract_named_entities([query], ner_pipeline)[0]
    # create embeddings for the query
    xq = retriever.encode(query).tolist()
    # query the lancedb table while applying named entity filter
    xc = lancedb_table.search(xq).to_list()
    # extract article titles from the search result
    r = [x["metadata"]["title"] for x in xc for i in x["metadata"]["named_entities"] if i in ne]
    return {"Extracted Named Entities": ne, "Result": r}
