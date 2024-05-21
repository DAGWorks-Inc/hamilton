from typing import Union

import lancedb
import numpy as np
import pyarrow as pa
from datasets import Dataset
from datasets.formatting.formatting import LazyBatch
from sentence_transformers import SentenceTransformer


def db_client() -> lancedb.DBConnection:
    """the lancedb client"""
    return lancedb.connect("./.lancedb")


def _write_to_lancedb(
    data: Union[list[dict], pa.Table], db: lancedb.DBConnection, table_name: str
) -> int:
    """Helper function to write to lancedb.

    This can handle the case the table exists or it doesn't.
    """
    try:
        db.create_table(table_name, data)
    except (OSError, ValueError):
        tbl = db.open_table(table_name)
        tbl.add(data)
    return len(data)


def _batch_write(dataset_batch: LazyBatch, db, table_name, other_columns) -> None:
    """Helper function to batch write to lancedb."""
    # we pull out the pyarrow table and select what we want from it
    _write_to_lancedb(
        dataset_batch.pa_table.select(["vector", "named_entities"] + other_columns), db, table_name
    )
    return None


def loaded_lancedb_table(
    final_dataset: Dataset,
    db_client: lancedb.DBConnection,
    table_name: str,
    metadata_of_interest: list[str],
    write_batch_size: int = 100,
) -> lancedb.table.Table:
    """Loads the data into lancedb explicitly -- but we lose some visibility this way.

    This function uses batching to write to lancedb.
    """
    final_dataset.map(
        _batch_write,
        batched=True,
        batch_size=write_batch_size,
        fn_kwargs={
            "db": db_client,
            "table_name": table_name,
            "other_columns": metadata_of_interest,
        },
        desc="writing to lancedb",
    )
    return db_client.open_table(table_name)


def lancedb_table(db_client: lancedb.DBConnection, table_name: str = "tw") -> lancedb.table.Table:
    """Table to query against"""
    tbl = db_client.open_table(table_name)
    return tbl


def lancedb_result(
    query: str,
    named_entities: list[str],
    retriever: SentenceTransformer,
    lancedb_table: lancedb.table.Table,
    top_k: int = 10,
    prefilter: bool = True,
) -> dict:
    """Result of querying lancedb.

    :param query: the query
    :param named_entities: the named entities found in the query
    :param retriever: the model to create the embedding from the query
    :param lancedb_table: the lancedb table to query against
    :param top_k: number of top results
    :param prefilter: whether to prefilter results before cosine distance
    :return: dictionary result
    """
    # create embeddings for the query
    query_vector = np.array(retriever.encode(query).tolist())

    # query the lancedb table
    query_builder = lancedb_table.search(query_vector, vector_column_name="vector")
    if named_entities:
        # applying named entity filter if something was returned
        where_clause = f"array_length(array_intersect({named_entities}, named_entities)) > 0"
        query_builder = query_builder.where(where_clause, prefilter=prefilter)
    result = (
        query_builder.select(["title", "url", "named_entities"])  # what to return
        .limit(top_k)
        .to_list()
    )
    # could rerank results here
    return {"Query": query, "Query Entities": named_entities, "Result": result}
