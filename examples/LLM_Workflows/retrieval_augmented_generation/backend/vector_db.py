import weaviate


def weaviate_client(vector_db_url: str) -> weaviate.Client:
    """Instantiate Weaviate client for the local instance based on the url"""
    client = weaviate.Client(vector_db_url)
    if client.is_live() and client.is_ready():
        return client
    else:
        raise ConnectionError("Error creating Weaviate client")


def full_schema() -> dict:
    return {
        "classes": [
            {
                "class": "Document",
                "description": "content of a PDF file",
                "vectorIndexType": "hnsw",
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "pdf_blob",
                        "dataType": ["blob"],
                        "description": "PDF file stored as base64 encoded blob",
                    },
                    {
                        "name": "file_name",
                        "dataType": ["text"],
                        "description": "file name of the original PDF file",
                    },
                    {
                        "name": "containsChunk",
                        "dataType": ["Chunk"],
                        "description": "chunk of text from the PDF document",
                    },
                ],
            },
            {
                "class": "Chunk",
                "description": "Chunk of an Document",
                "vectorIndexType": "hnsw",
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "fromDocument",
                        "dataType": ["Document"],
                        "description": "the Document containing this chunk",
                    },
                    {
                        "name": "chunk_index",
                        "dataType": ["int"],
                        "description": "the index of the chunk in the source document; starts at 0",
                    },
                    {
                        "name": "content",
                        "dataType": ["text"],
                        "description": "text content of this chunk",
                    },
                    {
                        "name": "summary",
                        "dataType": ["text"],
                        "description": "LLM-generated summary of the text content of this chunk",
                    },
                ],
            },
        ]
    }


def initialize_weaviate_instance(weaviate_client: weaviate.Client, full_schema: dict) -> dict:
    """Initialize Weaviate by creating the class schema"""
    if not weaviate_client.schema.contains(full_schema):
        weaviate_client.schema.create(full_schema)

    return {"schema_created": [class_["class"] for class_ in full_schema["classes"]]}


def reset_weaviate_storage(weaviate_client: weaviate.Client) -> bool:
    """Delete all schema and the data stored"""
    weaviate_client.schema.delete_all()
    return True


if __name__ == "__main__":
    # run as a script to test Weaviate + Hamilton locally
    import vector_db

    from hamilton import driver

    inputs = dict(
        vector_db_url="http://localhost:8083",
    )

    dr = driver.Builder().with_modules(vector_db).build()

    results = dr.execute(final_vars=["initialize_weaviate_instance"], inputs=inputs)
