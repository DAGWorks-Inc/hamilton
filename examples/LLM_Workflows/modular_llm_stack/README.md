# Flexibly change components of your LLM stack

This example shows how to pull data from the HuggingFace datasets hub, create embeddings for text passage using Cohere / OpenAI / SentenceTransformer, and store them in a vector database using LanceDB / Weaviate / Pinecone.

![](./weaviate_dag.png)
*DAG for OpenAI embeddings and Weaviate vector database*

In addition, you'll see how Hamilton can help you create replaceable components. This flexibility, makes it easier to assess service providers and refactor code to fit your needs. The above and below DAGs were generated simply by changing a string value and a module import. Try to spot the differences! 

![](./pinecone_dag.png)
*DAG for SentenceTransformers embeddings and Pinecone vector database*

# Example structure
- `run.py` contains the code to test the example. It uses `click` to provide a simple command interface. 
- `data_module.py` contains the code to pull data from HuggingFace. The code is in a separate Python module since it doesn't depend on the other functionalities and could include more involved preprocessing.
- `embedding_module.py` contains the code to embed text using either Cohere API, OpenAI API or SentenceTransformer library. The use of `@config.when` allows to have all options in the same Python module. This allows to quickly rerun your Hamilton DAG by simply changing your config. You'll see that functions share similar signature to enable interchangeability.
- `lancedb_module.py`, `weaviate_module.py` and `pinecone_module.py` implement the same functionalities for each vector database. Having the same function names allows Hamilton to abstract away the implementation details and reinforce the notion that both modules shouldn't be loaded simultaneously.
- `docker-compose.yml` allows you to start a local instance of Weaviate ([More information](https://weaviate.io/developers/weaviate/installation/docker-compose)).

# How-to run the example
Prerequesite:
- Create accounts and get the API keys for the service you plan to use.
- For Weaviate start your local instance using `docker compose up -d`
1. Run `python run.py --help` to learn about the options. You will options to:
    - Select a vector database from: weaviate, pinecone
    - Select an text embedding service from: openai, cohere, sentence_transformer
    - Select the text embedding model. There is a sensible default for each service.
    - Create a png visualization of the code execution.
2. Run `python run.py` to execute the code with `lancedb` and `sentence_transformer`

To change embedding service, you can use the following:
- SentenceTransformer: `--embedding_service=sentence_transformer --model_name=MODEL_NAME`
- OpenAI: `--embedding_service=openai --embedding_api_key=API_KEY`
- Cohere: `--embedding_service=openai --embedding_api_key=API_KEY`

To change vector database you need to pass a JSON config argument:
- LanceDB: `--vector_db=lancedb --vector_db_config='{"uri": "data/lancedb"}'`
- Weaviate: `--vector_db=weaviate --vector_db_config='{"url": "http://locahost:8080/"}'`
- Pinecone: `--vector_db=pinecone --vector_db_config='{"environment": "ENVIRONMENT", "api_key": "API_KEY"}'`

# Next step / Exercises
- Implement the code to read data from the vector database
- Add the code to send the same generative prompt to multiple providers 
