# Purpose of this module

This module shows a conversational retrieval augmented generation (RAG) example using
Hamilton. It shows you how you might structure your code with Hamilton to
create a RAG pipeline that takes into account conversation.

This example uses [FAISS](https://engineering.fb.com/2017/03/29/data-infrastructure/faiss-a-library-for-efficient-similarity-search/) + and in memory vector store and the OpenAI LLM provider.
The implementation of the FAISS vector store uses the LangChain wrapper around it.
That's because this was the simplest way to get this example up without requiring
someone having to host and manage a proper vector store.

The "smarts" in the is pipeline are that it will take a conversation, and then a question,
and then rewrite the question based on the conversation to be "standalone". That way
the standalone question can be used for the vector store query, as well as a more
specific question for the LLM given the found context.

## Example Usage

```python
# import the module
from hamilton import driver
from hamilton import lifecycle
dr = (
    driver.Builder()
    .with_modules(conversational_rag)
    .with_config({})
    # this prints the inputs and outputs of each step.
    .with_adapters(lifecycle.PrintLn(verbosity=2))
    .build()
)
# no chat history -- nothing to rewrite
result = dr.execute(
    ["conversational_rag_response"],
    inputs={
        "input_texts": [
            "harrison worked at kensho",
            "stefan worked at Stitch Fix",
        ],
        "question": "where did stefan work?",
        "chat_history": []
    },
)
print(result)

# this will now reword the question to then be
# used to query the vector store and the final LLM call.
result = dr.execute(
    ["conversational_rag_response"],
    inputs={
        "input_texts": [
            "harrison worked at kensho",
            "stefan worked at Stitch Fix",
        ],
        "question": "where did he work?",
        "chat_history": [
            "Human: Who wrote this example?",
            "AI: Stefan"
        ]
    },
)
print(result)
```

# How to extend this module
What you'd most likely want to do is:

1. Change the vector store (and how embeddings are generated).
2. Change the LLM provider.
3. Change the context and prompt.

With (1) you can import any vector store/library that you want. You should draw out
the process you would like, and that should then map to Hamilton functions.
With (2) you can import any LLM provider that you want, just use `@config.when` if you
want to switch between multiple providers.
With (3) you can add more functions that create parts of the prompt.

# Configuration Options
There is no configuration needed for this module.

# Limitations

You need to have the OPENAI_API_KEY in your environment.
It should be accessible from your code by doing `os.environ["OPENAI_API_KEY"]`.

The code does not check the context length, so it may fail if the context passed is too long
for the LLM you send it to.
