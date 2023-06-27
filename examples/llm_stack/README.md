# Evaluate components of your LLM stack

This example shows how to pull data from the HuggingFace datasets hub, create embeddings for text passage using Cohere / OpenAI / SentenceTransformer, and store them in a vector database using Pinecone / Weaviate.

In addition, you'll see how Hamilton can help you create replaceable components. With this flexibility, it becomes easy to test and assess service providers to find what fits your needs.

# Example structure
- `run.py` contains the code to test the example. You will need to add the necessary embedding API key or vector database configuration that you plan on using.
- `data_module.py` contains the code to pull data from HuggingFace. The code is in a separate Python module since it doesn't depend on the other functionalities and could include more involved preprocessing.
- `embedding_module.py` contains the code to embed text using either Cohere API, OpenAI API or SentenceTransformer library. The use of `@config.when` allows to have all options in the same Python module. This allows to quickly rerun your Hamilton DAG by simply changing your config. You'll see that functions share similar signature to enable interchangeability.
- `weaviate_module.py` and `pinecone_module.py` implement the same functionalities for each vector database. Having the same function names allows Hamilton to abstract away the implementation details and reinforce the notion that both modules shouldn't be loaded simultaneously.
- `docker-compose.yml` allows you to start a local instance of Weaviate ([More information](https://weaviate.io/developers/weaviate/installation/docker-compose)).
