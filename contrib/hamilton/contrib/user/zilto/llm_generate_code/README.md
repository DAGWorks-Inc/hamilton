# Purpose of this module

This module uses the OpenAI completion API to generate code.

For any language, you can request `generated_code` to get the generated response. If you are generating Python code, you can execute it in a subprocess by requesting `execution_output` and `execution_error`.

## Example
```python
from hamilton import driver
import __init__ as llm_generate_code

dr = driver.Builder().with_modules(llm_generate_code).build()

dr.execute(
    ["execution_output", "execution_error"],
    inputs=dict(
        query="Retrieve the primary type from a `typing.Annotated` object`",
    )
)
```

## Configuration Options
### Config.when
This module doesn't receive configurations.

### Inputs
- `query`: The query for which you want code generated.
- `api_key`: Set the OpenAI API key to use. If None, read the environment variable `OPENAI_API_KEY`
- `code_language`: Set the code language to generate the reponse in. Defaults to `python`

### Overrides
- `prompt_template_to_generate_code`: Create a new prompt template with the fields `query` and `code_language`.
- `prompt_to_generate_code`: Manually provide a prompt to generate Python code

## Extension / Limitations
- Executing arbitrary generated code is a security risk. Proceed with caution.
- You need to manually install dependencies for your generated code to be executed (i.e., you need to `pip install pandas` yourself)
