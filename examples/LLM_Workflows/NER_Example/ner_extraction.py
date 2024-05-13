from typing import Union

import lancedb
import numpy as np

# import pandas as pd
import torch
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict, load_dataset
from sentence_transformers import SentenceTransformer
from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline
from transformers.pipelines import base

HF_DS = Union[DatasetDict, Dataset, IterableDatasetDict, IterableDataset]


def medium_articles() -> HF_DS:
    # load the dataset and convert to pandas dataframe
    ds = load_dataset("fabiochiu/medium-articles", data_files="medium_articles.csv", split="train")
    # change to HF datasetset
    return ds


def sampled_articles(
    medium_articles: HF_DS,
    sample_size: int = 1000,
    random_state: int = 32,
    max_text_length: int = 1000,
) -> HF_DS:
    """Samples the articles and does some light transformations.
    Transformations:
     - selects the first 1000 characters of text. This is for performance here. But in real life you'd
     do something for your use case.
      - Joins article title and the text to create one text string.
    """
    # Filter out entries with NaN values in 'text' or 'title' fields
    dataset = medium_articles.filter(
        lambda example: example["text"] is not None and example["title"] is not None
    )

    # Shuffle and take the first 10000 samples
    dataset = dataset.shuffle(seed=random_state).select(range(sample_size))

    # Truncate the 'text' to the first 1000 characters
    dataset = dataset.map(lambda example: {"text": example["text"][:max_text_length]})

    # Concatenate the 'title' and truncated 'text'
    dataset = dataset.map(lambda example: {"title_text": example["title"] + ". " + example["text"]})
    return dataset


def device() -> str:
    return "cuda" if torch.cuda.is_available() else "cpu"


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
def ner_pipeline(model: object, tokenizer: AutoTokenizer, device: str) -> base.Pipeline:
    print("Loading the ner_pipeline")
    device_no = torch.cuda.current_device() if device == "cuda" else None
    return pipeline(
        "ner", model=model, tokenizer=tokenizer, aggregation_strategy="max", device=device_no
    )


def retriever(device: str) -> SentenceTransformer:
    """A retriever model is used to embed passages (article title + first 1000 characters) and queries. It creates embeddings such that queries and passages with similar meanings are close in the vector space. We will use a sentence-transformer model as our retriever. The model can be loaded as follows:"""
    print("Loading the retriever model")
    return SentenceTransformer("flax-sentence-embeddings/all_datasets_v3_mpnet-base", device=device)


def db() -> lancedb.DBConnection:
    return lancedb.connect("./.lancedb")


from tqdm.auto import tqdm


def _extract_named_entities(text_batch, ner_pipeline):
    # extract named entities using the NER pipeline
    extracted_batch = ner_pipeline(text_batch)
    # this should be extracted_batch = dataset.map(ner_pipeline)
    entities = []
    # loop through the results and only select the entity names
    for text in extracted_batch:
        ne = [entity["word"] for entity in text]
        entities.append(ne)
    return entities


def _extract_named_entities_text(title_text, ner_pipeline):
    # extract named entities using the NER pipeline
    extracted_batch = ner_pipeline(title_text)
    # this should be extracted_batch = dataset.map(ner_pipeline)
    entities = []
    # loop through the results and only select the entity names
    for text in extracted_batch:
        ne = [entity["word"] for entity in text]
        entities.append(ne)
    named_entities = [list(set(entity)) for entity in entities]
    return named_entities


# def embedded_dataset(sampled_articles: HF_DS, retriever: SentenceTransformer) -> HF_DS:
#     return sampled_articles.map(lambda x: {"vector": retriever.encode(x["title_text"])})


def metadata_of_interest() -> list[str]:
    return ["title", "url", "authors", "timestamp", "tags"]


def final_dataset(
    sampled_articles: HF_DS,
    retriever: SentenceTransformer,
    ner_pipeline: base.Pipeline,
    metadata_of_interest: list[str],
) -> HF_DS:
    def _batch_map(dataset, retriever, ner_pipeline) -> dict:
        title_text_list = dataset["title_text"]
        emb = retriever.encode(title_text_list)
        named_entities = _extract_named_entities_text(title_text_list, ner_pipeline)
        # _df = dataset.to_pandas()
        # _df = _df.drop("title_text", axis=1)
        return {
            "embedding": emb,
            # "metadata": _df.to_dict(orient="records"),
            "named_entities": named_entities,
        }

    final_ds = sampled_articles.map(
        _batch_map, batched=True, fn_kwargs={"retriever": retriever, "ner_pipeline": ner_pipeline}
    )

    final_ds = final_ds.map(lambda x: {"metadata": {k: x[k] for k in metadata_of_interest}})
    return final_ds


def _write_to_lancedb(data: list[dict], db: lancedb.DBConnection, table_name: str) -> int:
    try:
        db.create_table(table_name, data)
    except (OSError, ValueError):
        tbl = db.open_table(table_name)
        tbl.add(data)
    return len(data)


def load_into_lance_db(
    final_dataset: HF_DS, db: lancedb.DBConnection, table_name: str, write_batch_size: int = 100
) -> int:

    # iterate in batches to write to lancedb
    data = []
    total_written = 0
    for i in tqdm(range(0, len(final_dataset), write_batch_size)):
        # find end of batch
        i_end = min(i + write_batch_size, len(final_dataset))
        # extract batch
        batch = final_dataset[i:i_end]
        # create unique IDs
        # add all to upsert list
        to_upsert = list(zip(batch["embedding"], batch["metadata"], batch["named_entities"]))
        for emb, meta, entity in to_upsert:
            temp = {}
            temp["vector"] = np.array(emb)
            temp["metadata"] = meta
            temp["named_entities"] = entity
            data.append(temp)
        total_written += _write_to_lancedb(data, db, table_name)
        data = []
    return total_written


# def lancedb_data(
#     sampled_articles: HF_DS,
#     retriever: SentenceTransformer,
#     batch_size: int,
#     ner_pipeline: base.Pipeline,
# ) -> list[dict]:
#     data = []
#
#     dataset = sampled_articles.map(_batch_map, batched=True, fn_kwargs={"retriever": retriever, "ner_pipeline": ner_pipeline})
#
#     for i in tqdm(range(0, len(sampled_articles), batch_size)):
#         # find end of batch
#         i_end = min(i + batch_size, len(sampled_articles))
#         # extract batch
#         batch = sampled_articles.iloc[i:i_end].copy()
#         # generate embeddings for batch
#         # def _title_text_list(batch):
#         #     return retriever.encode(batch["title_text"].tolist()).tolist()
#         # emb = dataset.map(_title_text_list)
#         emb = retriever.encode(batch["title_text"].tolist()).tolist()
#         # extract named entities from the batch
#         entities = _extract_named_entities(batch["title_text"].tolist(), ner_pipeline)
#         # remove duplicate entities from each record
#         batch["named_entities"] = [list(set(entity)) for entity in entities]
#         batch = batch.drop("title_text", axis=1)
#         # get metadata
#         meta = batch.to_dict(orient="records")
#         # create unique IDs
#         ids = [f"{idx}" for idx in range(i, i_end)]
#         # add all to upsert list
#         to_upsert = list(zip(ids, emb, meta, batch["named_entities"]))
#         for id, emb, meta, entity in to_upsert:
#             temp = {}
#
#             temp["vector"] = np.array(emb)
#             temp["metadata"] = meta
#             temp["named_entities"] = entity
#             data.append(temp)
#     return data


# def load_into_lancedb(
#     lancedb_data: list[dict],
#     db: lancedb.DBConnection,
#     table_name: str,
# ) -> int:
#     try:
#         db.create_table(table_name, lancedb_data)
#     except (OSError, ValueError):
#         tbl = db.open_table(table_name)
#         tbl.add(lancedb_data)
#     return len(lancedb_data)


def lancedb_table(db: lancedb.DBConnection, table_name: str = "tw") -> lancedb.table.Table:
    tbl = db.open_table(table_name)
    return tbl


def named_entities(query: str, ner_pipeline: base.Pipeline) -> list[str]:
    return _extract_named_entities([query], ner_pipeline)[0]


def search_lancedb(
    query: str,
    named_entities: list[str],
    retriever: SentenceTransformer,
    lancedb_table: lancedb.table.Table,
) -> dict:
    # extract named entities from the query
    # ne = _extract_named_entities([query], ner_pipeline)[0]
    # print(ne)
    # create embeddings for the query
    xq = retriever.encode(query).tolist()
    # query the lancedb table while applying named entity filter
    xc = lancedb_table.search(xq).to_list()
    # extract article titles from the search result
    r = [x["metadata"]["title"] for x in xc for i in x["named_entities"] if i in named_entities]
    return {"Extracted Named Entities": named_entities, "Result": r, "Query": query}
