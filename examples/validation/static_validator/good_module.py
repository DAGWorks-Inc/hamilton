from hamilton.function_modifiers import tag


@tag(node_type="output", table_name="my_table")
def foo() -> str:
    return "Hello, world!"
