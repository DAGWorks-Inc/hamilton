# hamilton_async.py
from typing import List

import openai


def llm_client() -> openai.AsyncOpenAI:
    return openai.AsyncOpenAI()


def joke_prompt(topic: str) -> str:
    return f"Tell me a short joke about {topic}"


def joke_messages(joke_prompt: str) -> List[dict]:
    return [{"role": "user", "content": joke_prompt}]


async def joke_response(llm_client: openai.AsyncOpenAI, joke_messages: List[dict]) -> str:
    response = await llm_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=joke_messages,
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    import asyncio

    import hamilton_async

    from hamilton import base
    from hamilton.experimental import h_async

    dr = h_async.AsyncDriver({}, hamilton_async, result_builder=base.DictResult())
    dr.display_all_functions("hamilton-async.png")
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(dr.execute(["joke_response"], inputs={"topic": "ice cream"}))
    print(result)
