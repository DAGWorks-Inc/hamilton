from hamilton.function_modifiers import tag


@tag(node_type="output")
def foo() -> str:
    return "Hello, world!"
