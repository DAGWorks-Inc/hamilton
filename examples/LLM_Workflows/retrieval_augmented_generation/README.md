# Retrieval Augmented Generation app with Hamilton
Here's a retrieval augmented generation (RAG) application that allows users to import PDF files, extract and store their text content, and ask questions over the documents. Each operation is implemented as a dataflow with Hamilton and is exposed via a FastAPI endpoint. This backend uses the OpenAI API to embed documents and generate answers, and uses a local Weaviate vector store instance to store and retrieve documents. The frontend is built with Streamlit and exposes the different functionalities via a simple web UI. Everything is packaged as containers with docker compose. This example draws from previous simpler examples ([Knowledge Retrieval](), [Modular LLM Stack](), [PDF Summarizer]()).

> Find below a list of references for the technical concepts found in this example


# Setup
1. Clone this repository `git clone https://github.com/dagworks-inc/hamilton.git`
2. Move to the directory `cd hamilton/examples/LLM_Workflows/retrieval_augmented_generation`
3. Create a `.env` (next to `README.md` and `docker-compose.yaml`) and add your OpenAI API key in  such that `OPENAI_API_KEY=YOUR_API_KEY`
4. Build docker images `docker compose build`
5. Create docker containers `docker compose up -d`
6. Go to [http://localhost:8081/](http://localhost:8081/) to view the Streamlit app. If it's running, it means the FastAPI server started and was able to connect to Weaviate. You can manually verify FastAPI at [http://localhost:8082/docs](http://localhost:8082/docs) and Weaviate at [http://localhost:8080/docs](http://localhost:8083/v1) 
7. If you make changes, you need to rebuild the docker images, so do `docker compose up -d --build`.
8. To stop the containers do `docker compose down`.
9. To look at the logs, your docker application should allow you to view them,
or you can do `docker compose logs -f` to tail the logs (ctrl+c to stop tailing the logs).

## Connecting to DAGWorks
1. Create a DAGWorks account at www.dagworks.io - follow the instructions to set up a project.
2. Add your DAGWorks API Key to the `.env` file. E.g. `DAGWORKS_API_KEY=YOUR_API_KEY`
3. Uncomment dagworks-sdk in `requirements.txt`.
4. Uncomment the lines in server.py to replace `sync_dr` with the DAGWorks Driver.
5. Rebuild the docker images `docker compose up -d --build`.

# Technical References:
- [Docker Compose Networking](https://docs.docker.com/compose/networking/)
- [FastAPI /docs metadata](https://fastapi.tiangolo.com/tutorial/metadata/)
- [FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/)
- [Hamilton Interactive online demo](https://www.tryhamilton.dev/)
- [Hamilton Parallelizable/Collect](https://hamilton.dagworks.io/en/latest/concepts/customizing-execution/#dynamic-dags-parallel-execution)
- [OpenAI API docs](https://platform.openai.com/docs/introduction)
- [RAG sample architecture by Amazon](https://docs.aws.amazon.com/sagemaker/latest/dg/jumpstart-foundation-models-customize-rag.html)
- [Streamlit API documentation](https://docs.streamlit.io/library/api-reference)
- [Streamlit Include PDF as HTML within Markdown](https://discuss.streamlit.io/t/rendering-pdf-on-ui/13505)
- [Weaviate 101](https://weaviate.io/developers/weaviate/tutorials)
- [Weaviate GraphQL Retrieval API](https://weaviate.io/developers/weaviate/api/graphql#graphql)
- [Weaviate Python Client](https://weaviate-python-client.readthedocs.io/en/stable/index.html)
- [Weaviate Hybrid Search](https://weaviate.io/developers/academy/zero_to_mvp/queries_2/hybrid)
