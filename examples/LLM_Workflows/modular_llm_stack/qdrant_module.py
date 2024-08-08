import numpy as np
from qdrant_client import QdrantClient, models


def client_vector_db(vector_db_config: dict) -> QdrantClient:
    return QdrantClient(**vector_db_config)


def initialize_vector_db_indices(
    client_vector_db: QdrantClient,
    class_name: str,
    embedding_dimension: int,
) -> bool:
    if client_vector_db.collection_exists(class_name):
        client_vector_db.delete_collection(class_name)

    client_vector_db.create_collection(
        class_name,
        vectors_config=models.VectorParams(
            size=embedding_dimension, distance=models.Distance.COSINE
        ),
    )

    return True


def reset_vector_db(client_vector_db: QdrantClient) -> bool:
    for collection in client_vector_db.get_collections().collections:
        client_vector_db.delete_collection(collection.name)
    return True


def data_objects(
    ids: list[str],
    titles: list[str],
    text_contents: list[str],
    embeddings: list[np.ndarray],
    metadata: dict,
) -> dict:
    assert len(ids) == len(titles) == len(embeddings)
    # Qdrant only allows unsigned integers and UUIDs as point IDs
    ids = list(range(len(ids)))
    payloads = [
        dict(id=_id, text_content=text_content, title=title, **metadata)
        for _id, title, text_content in zip(ids, titles, text_contents)
    ]
    embeddings = [x.tolist() for x in embeddings]
    return dict(ids=ids, vectors=embeddings, payload=payloads)


def push_to_vector_db(
    client_vector_db: QdrantClient,
    class_name: str,
    data_objects: dict,
) -> None:
    client_vector_db.upload_collection(class_name, **data_objects)
