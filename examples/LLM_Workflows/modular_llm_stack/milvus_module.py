from types import ModuleType

import numpy as np
import pymilvus
from pymilvus import CollectionSchema, DataType, FieldSchema


def vector_db(vector_db_config: dict) -> ModuleType:
    """Instantiate Milvus connection."""
    pymilvus.connections.connect(**vector_db_config)
    return pymilvus


def collection_name(class_name: str) -> str:
    """Convert class_name to valid Milvus collection_name"""
    return class_name.lower()


def initialize_vector_db_indices(
    vector_db: ModuleType,
    collection_name: str,
    embedding_dimension: int,
    embedding_metric: str,
    index_type: str = "IVF_FLAT",
    index_params: dict = None,
) -> bool:
    """Initialize Milvus by creating the collection"""
    if index_params is None:
        index_params = {"nlist": 128}

    fields = [
        FieldSchema(name="id", dtype=DataType.STRING, is_primary=True, auto_id=False),
        FieldSchema(name="titles", dtype=DataType.STRING),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=embedding_dimension),
    ]
    schema = CollectionSchema(fields, "Fields for the collection.")
    milvus_collection = vector_db.Collection(collection_name, schema)

    index = {
        "index_type": index_type,
        "metric_type": embedding_metric,
        "params": index_params,
    }

    milvus_collection.create_index("embeddings", index)
    return True


def reset_vector_db(vector_db: ModuleType) -> bool:
    """Delete the entire index and the data stored"""
    for idx in vector_db.list_collections():
        vector_db.drop_collection(idx)
    return True


def data_objects(ids: list[str], titles: list[str], embeddings: list[np.ndarray]) -> list[dict]:
    """Create valid Milvus objects (index, title, embeddings) for insertion into Milvus."""
    assert len(ids) == len(titles) == len(embeddings)

    objects = []
    for id, title, embedding in zip(ids, titles, embeddings):
        objects.append(
            {
                "id": id,
                "title": title,
                "embedding": embedding,
            }
        )
    return objects


def push_to_vector_db(
    vector_db: ModuleType,
    collection_name: str,
    data_objects: list[dict],
    batch_size: int = 100,
) -> int:
    """Insert objects to Milvus index; return the number of objects inserted"""
    collection = vector_db.Collection(collection_name)

    for i in range(0, len(data_objects), batch_size):
        i_end = min(i + batch_size, len(data_objects))

        collection.insert(vectors=data_objects[i:i_end])
    collection.flush()
    return len(data_objects)
