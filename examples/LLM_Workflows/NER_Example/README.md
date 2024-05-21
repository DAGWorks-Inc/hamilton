# Document processing with Named Entity Recognition (NER) for RAG
This example demonstrates how to use the Named Entity Recognition (NER) model to extract entities from a document.
This extra metadata can be used when querying over the documents in the RAG model to filter to the documents
that contain the entities of interest.

The pipeline we create can be seen in the image below.
![pipeine](ner_extraction_pipeline.png)

To run this example:
1. Install the requirements by running `pip install -r requirements.txt`
2. Run the script `python run.py`. Some example commands:

    - python run.py medium_docs load
    - python run.py medium_docs query --query "Why does SpaceX want to build a city on Mars?"
    - python run.py medium_docs query --query "How are autonomous vehicles changing the world?"

3. To see the full list of commands run `python run.py --help`.
