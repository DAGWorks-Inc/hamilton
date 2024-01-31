import vanilla_anthropic
import vanilla_completion


def invoke_chain_with_fallback(topic: str) -> str:
    try:
        return vanilla_completion.invoke_chain(topic)
    except Exception:
        return vanilla_anthropic.invoke_anthropic_chain(topic)


if __name__ == "__main__":
    print(invoke_chain_with_fallback("ice cream"))
