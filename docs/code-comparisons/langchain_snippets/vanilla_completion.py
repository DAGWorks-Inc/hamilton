import openai

prompt_template = "Tell me a short joke about {topic}"
client = openai.OpenAI()


def call_llm(prompt_value: str) -> str:
    response = client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=prompt_value,
    )
    return response.choices[0].text

def invoke_llm_chain(topic: str) -> str:
    prompt_value = prompt_template.format(topic=topic)
    return call_llm(prompt_value)



if __name__ == "__main__":
    print(invoke_llm_chain("ice cream"))
