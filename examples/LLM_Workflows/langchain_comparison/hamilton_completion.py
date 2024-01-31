# hamilton_completion.py
from typing import List

import openai

from hamilton.function_modifiers import config


def llm_client() -> openai.OpenAI:
    return openai.OpenAI()


def joke_prompt(topic: str) -> str:
    return f"Tell me a short joke about {topic}"


def joke_messages(joke_prompt: str) -> List[dict]:
    return [{"role": "user", "content": joke_prompt}]


@config.when(type="completion")
def joke_response__completion(llm_client: openai.OpenAI, joke_prompt: str) -> str:
    response = llm_client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=joke_prompt,
    )
    return response.choices[0].text


@config.when(type="chat")
def joke_response__chat(llm_client: openai.OpenAI, joke_messages: List[dict]) -> str:
    response = llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=joke_messages,
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    import hamilton_completion

    from hamilton import driver

    dr = (
        driver.Builder()
        .with_modules(hamilton_completion)
        .with_config({"type": "completion"})
        .build()
    )
    dr.display_all_functions("hamilton-completion.png")
    print(dr.execute(["joke_response"], inputs={"topic": "ice cream"}))

    dr = driver.Builder().with_modules(hamilton_completion).with_config({"type": "chat"}).build()
    dr.display_all_functions("hamilton-chat.png")
    print(dr.execute(["joke_response"], inputs={"topic": "ice cream"}))
