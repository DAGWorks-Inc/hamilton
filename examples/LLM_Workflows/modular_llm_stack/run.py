import json

import click
import data_module
import embedding_module
import lancedb_module
import pinecone_module  # noqa: F401
import weaviate_module

from hamilton import base, driver


@click.command()
@click.option(
    "--vector_db",
    type=click.Choice(["lancedb", "weaviate", "pinecone"], case_sensitive=False),
    default="lancedb",
    help="Vector database service",
)
@click.option(
    "--vector_db_config",
    default='{"uri": "data/lancedb"}',
    help="Pass a JSON string for vector database config.\
        Weaviate needs a dictionary {'url': ''}\
        Pinecone needs dictionary {'environment': '', 'api_key': ''}",
)
@click.option(
    "--embedding_service",
    type=click.Choice(["openai", "cohere", "sentence_transformer"], case_sensitive=False),
    default="sentence_transformer",
    help="Text embedding service.",
)
@click.option(
    "--embedding_service_api_key",
    default=None,
    help="API Key for embedding service. Needed if using OpenAI or Cohere.",
)
@click.option("--model_name", default=None, help="Text embedding model name.")
@click.option("--display_dag", is_flag=True, help="Generate a .png of the Hamilton DAG")
def main(
    vector_db: str,
    vector_db_config: str,
    embedding_service: str,
    embedding_service_api_key: str | None,
    model_name: str,
    display_dag: bool,
):
    if vector_db == "weaviate":
        vector_db_module = weaviate_module
    elif vector_db == "pinecone":
        vector_db_module = pinecone_module
    elif vector_db == "lancedb":
        vector_db_module = lancedb_module

    if model_name is None:
        if embedding_service == "openai":
            model_name = "text-embedding-ada-002"
        elif embedding_service == "cohere":
            model_name = "embed-english-light-v2.0"
        elif embedding_service == "sentence_transformer":
            model_name = "multi-qa-MiniLM-L6-cos-v1"

    config = dict(
        vector_db_config=json.loads(vector_db_config),
        embedding_service=embedding_service,  # this triggers config.when() in embedding_module
        api_key=embedding_service_api_key,
        model_name=model_name,
    )

    dr = driver.Driver(
        config,
        data_module,
        vector_db_module,  # this points to weaviate_module or pinecone_module
        embedding_module,
        adapter=base.SimplePythonGraphAdapter(base.DictResult()),
    )

    # The `final_vars` requested are functions with side-effects
    dr.execute(
        final_vars=["initialize_vector_db_indices", "push_to_vector_db"],
        inputs=dict(
            class_name="SQuADEntry",
        ),
    )

    if display_dag:
        filename = f"{vector_db}_{embedding_service}_dag"
        dr.display_all_functions(filename, {"format": "png"})


if __name__ == "__main__":
    main()
