from typing import List

import openai

prompt_template = "Tell me a short joke about {topic}"
client = openai.OpenAI()


def call_chat_model(messages: List[dict]) -> str:
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
    )
    return response.choices[0].message.content


def invoke_chain(topic: str) -> str:
    prompt_value = prompt_template.format(topic=topic)
    messages = [{"role": "user", "content": prompt_value}]
    return call_chat_model(messages)


if __name__ == "__main__":
    print(invoke_chain("ice cream"))
