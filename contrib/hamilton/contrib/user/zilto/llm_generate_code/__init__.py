import logging
import os
import subprocess
from typing import Optional

from hamilton.function_modifiers import extract_fields

logger = logging.getLogger(__name__)

from hamilton import contrib

with contrib.catch_import_errors(__name__, __file__, logger):
    import openai


def llm_client(api_key: Optional[str] = None) -> openai.OpenAI:
    """Create an OpenAI client"""
    if api_key is None:
        api_key = os.environ.get("OPENAI_API_KEY")

    return openai.OpenAI(api_key=api_key)


def prompt_template_to_generate_code() -> str:
    return """Write some {code_language} code to solve the user's problem.

Return only python code in Markdown format, e.g.:

```{code_language}
....
```

user problem
{query}

{code_language} code
"""


def prompt_to_generate_code(
    prompt_template_to_generate_code: str, query: str, code_language: str = "python"
) -> str:
    return prompt_template_to_generate_code.format(
        query=query,
        code_language=code_language,
    )


def response_generated_code(llm_client: openai.OpenAI, prompt_to_generate_code: str) -> str:
    response = llm_client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=prompt_to_generate_code,
    )
    return response.choices[0].text


def generated_code(response_generated_code: str) -> str:
    _, _, lower_part = response_generated_code.partition("```python")
    code_part, _, _ = lower_part.partition("```")
    return code_part


def code_prepared_for_execution(generated_code: str, code_language: str = "python") -> str:
    if code_language != "python":
        raise ValueError("Can only execute the generated code if `code_language` = 'python'")

    code_to_get_vars = """
excluded_vars = { 'excluded_vars', '__builtins__', '__annotations__'} | set(dir(__builtins__))
local_vars = {k:v for k,v in locals().items() if k not in excluded_vars}
print(local_vars)
"""
    return generated_code + code_to_get_vars


@extract_fields(
    dict(
        execution_output=str,
        execution_error=str,
    )
)
def execute_output(code_prepared_for_execution: str) -> dict:
    process = subprocess.Popen(
        ["python", "-c", code_prepared_for_execution],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    output, errors = process.communicate()
    return dict(execution_output=output, execution_error=errors)


# run as a script to test dataflow
if __name__ == "__main__":
    import __init__ as llm_generate_code

    from hamilton import driver

    dr = driver.Builder().with_modules(llm_generate_code).build()

    dr.display_all_functions("dag.png", orient="TB")

    res = dr.execute(
        ["execution_output", "execution_error"],
        overrides=dict(generated_code="s = 'hello world'"),
    )

    print(res)
