# hamilton_batch.py
from typing import List

import openai

from hamilton.execution import executors
from hamilton.htypes import Collect, Parallelizable


def llm_client() -> openai.OpenAI:
    return openai.OpenAI()


def topic(topics: list[str]) -> Parallelizable[str]:
    for _topic in topics:
        yield _topic


def joke_prompt(topic: str) -> str:
    return f"Tell me a short joke about {topic}"


def joke_messages(joke_prompt: str) -> List[dict]:
    return [{"role": "user", "content": joke_prompt}]


def joke_response(llm_client: openai.OpenAI, joke_messages: List[dict]) -> str:
    response = llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=joke_messages,
    )
    return response.choices[0].message.content


def joke_responses(joke_response: Collect[str]) -> List[str]:
    return list(joke_response)


if __name__ == "__main__":
    import hamilton_batch

    from hamilton import driver

    dr = (
        driver.Builder()
        .with_modules(hamilton_batch)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_remote_executor(executors.MultiThreadingExecutor(5))
        .build()
    )
    dr.display_all_functions("hamilton-batch.png")
    print(
        dr.execute(["joke_responses"], inputs={"topics": ["ice cream", "spaghetti", "dumplings"]})
    )

    # can still run single chain with overrides
    # and getting just one response
    print(dr.execute(["joke_response"], overrides={"topic": "lettuce"}))
