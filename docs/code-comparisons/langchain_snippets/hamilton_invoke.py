# hamilton_invoke.py
from typing import List

import openai


def llm_client() -> openai.OpenAI:
    return openai.OpenAI()


def joke_prompt(topic: str) -> str:
    return f"Tell me a short joke about {topic}"


def joke_messages(joke_prompt: str) -> List[dict]:
    return [{"role": "user", "content": joke_prompt}]


def joke_response(llm_client: openai.OpenAI,
                  joke_messages: List[dict]) -> str:
    response = llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=joke_messages,
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    import hamilton_invoke

    from hamilton import driver

    dr = (
        driver.Builder()
        .with_modules(hamilton_invoke)
        .build()
    )
    dr.display_all_functions("hamilton-invoke.png")
    print(dr.execute(["joke_response"],
                     inputs={"topic": "ice cream"}))
