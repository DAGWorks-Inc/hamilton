import anthropic

prompt_template = "Tell me a short joke about {topic}"
anthropic_template = f"Human:\n\n{prompt_template}\n\nAssistant:"
anthropic_client = anthropic.Anthropic()


def call_anthropic(prompt_value: str) -> str:
    response = anthropic_client.completions.create(
        model="claude-2",
        prompt=prompt_value,
        max_tokens_to_sample=256,
    )
    return response.completion


def invoke_anthropic_chain_with_logging(topic: str) -> str:
    print(f"Input: {topic}")
    prompt_value = anthropic_template.format(topic=topic)
    print(f"Formatted prompt: {prompt_value}")
    output = call_anthropic(prompt_value)
    print(f"Output: {output}")
    return output



if __name__ == "__main__":
    print(invoke_anthropic_chain_with_logging("ice cream"))
