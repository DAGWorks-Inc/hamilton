import data_module
import embedding_module
import pinecone_module  # noqa: F401
import weaviate_module

from hamilton import base, driver


def main():
    weaviate_config = {"url": "http://localhost:8080/"}
    # pinecone_config = {
    #     "environment": "PINECONE_ENVIRONMENT",
    #     "api_key": "PINECONE_API_KEY",
    # }

    config = dict(
        vector_db_config=weaviate_config,
        embedding_service="(openai,cohere,sentence_transformer)",  # this triggers config.when() in embedding_module
        api_key="EMBEDDING_SERVICE_API_KEY",  # needed if using OpenAI or Cohere
        model_name="text-embedding-ada-002",
    )

    dr = driver.Driver(
        config,
        data_module,
        weaviate_module,  # this can be swapped for `pinecone_module`
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


if __name__ == "__main__":
    main()
