from typing import Iterator, List

import openai

prompt_template = "Tell me a short joke about {topic}"
client = openai.OpenAI()


def stream_chat_model(messages: List[dict]) -> Iterator[str]:
    stream = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
        stream=True,
    )
    for response in stream:
        content = response.choices[0].delta.content
        if content is not None:
            yield content


def stream_chain(topic: str) -> Iterator[str]:
    prompt_value = prompt_template.format(topic=topic)
    return stream_chat_model([{"role": "user", "content": prompt_value}])


if __name__ == "__main__":
    for chunk in stream_chain("ice cream"):
        print(chunk, end="", flush=True)
