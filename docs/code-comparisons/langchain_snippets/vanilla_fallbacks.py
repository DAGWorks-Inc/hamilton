def invoke_chain_with_fallback(topic: str) -> str:
    try:
        return invoke_chain(topic)  # noqa: F821
    except Exception:
        return invoke_anthropic_chain(topic)  # noqa: F821

if __name__ == '__main__':
    print(invoke_chain_with_fallback("ice cream"))
