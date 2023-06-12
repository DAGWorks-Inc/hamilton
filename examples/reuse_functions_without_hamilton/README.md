# Reusing Hamilton Functions without depending on Hamilton
Here we show how to define transformation logic in a module without Hamilton dependencies (`functions_module`) and add Hamilton decorators via another Python file (`decorators_module`). This example is in response to this [GitHub issue](https://github.com/DAGWorks-Inc/hamilton/issues/50). However, it showcases more general mechanisms of how to parse and dynamically alter source code.

# Use case
Your organization defines key data transformations in a centralized repository to promote consistency across teams and applications. However, file can't be edited and Hamilton can't be added as a dependency.

**Solution**: Create a file `external_decorators.py` where you define decorators to add to functions from another file. At runtime, we will combine both to create a temporary module with decorated functions.


# File organization
- **Developer utilities**: `run.py` shows how to use `remove_hamilton()` to undecorate and `use_external_decorators()` to decorate function modules. `decorator_utilities.py` contains the code powering these functionalities. The section under `if __name__ == "__main__":` shows a step-by-step explanation of the inner workings.

- **User defined**: Files created by a user during its project. This includes `my_functions.py` which contains the feature definitions (without Hamilton dependencies) and `external_decorators.py` that assigns decorators to function stubs.

- **Generated**: Files created programmatically by using `decorator_utilities` features. These files can be deleted and regenerated at \*no\* cost. This includes `.py` files with `__decorated` or `__undecorated` suffixes.


# How it works (TL;DR)
1. We read the Python files as text inputs, then parse it with Python's strict grammar (abstract syntax tree).
2. Since the function names in `my_functions.py` and `external_decorators.py` are matching, we can precisely take decorators from one file and add it to the other.
3. We load the edited code as a temporary Python module and its decorated functions become available for use!

Detailed explanation in `decorator_utilities.py` under `if __name__ == "__main__":`.


# Usage patterns and suggestions
- Since DAG is defined through 2 files, proper version control requires care. Here's two approach:
    1. Your code always build the decorated temporary module at runtime. By properly tracking the `functions_module` and the `decorators_module` it is possible to infer the decorated temporary module and rebuild it on demand. The main downside is that on failures, you need access to both files to generated the decorated module for debugging and tracing errors.
    2. Your code relies on a saved version of the decorated temporary module that is regenerated on changes to `functions_module` or `decorators_module`. This can lead to more predictable behavior and easier bug tracing since the decorated file is easily accessible. However, this file can become out-of-sync if someone directly edits it or it isn't regenerated after edits to the `functions_module` and `decorators_module`.
- Run your linter, debugger, etc. on generated files
- The Python built-in packages `importlib`, `ast`, `types`, `typing` are being actively developed and subject to changes. Python version control is important if your system depends on these features.


# Edge cases and limitations

- **Issue**: Trying to import a `.py` file containing decorated functions with missing type annotations will throw an error. This is true for certain decorators such as `@extract_columns()`, but not other such as `@check_output()`.
    - **Solution 1**: Add annotations to the decorated stubs (see `seasons_encoded` in `external_decorators.py`)
    - **Solution 2**: Provide `str` or `Path` arguments instead of `ModuleType` to `remove_hamilton()` and `use_external_decorators()`. This way, the modules will be read and edited without importing it, therefore avoiding triggering errors.

        Use:
        ```
        decorated_module = use_external_decorators(
            source_module="./my_functions__undecorated.py",
            decorators_module="./external_decorators.py",
            save_module=False,
        )
        ```

        Instead of:
        ```
        import my_functions__undecorated
        import external_decorators

        decorated_module = use_external_decorators(
            functions_module=my_functions__undecorated,
            decorators_module=external_decorators,
            save_module=True,
        )
        ```
- Decorators from `decorators_module` are applied in "on top" / "on the outside" of the existing decorators in `functions_module` if any.
- The function `use_external_decorators(functions_module, decorators_module)` will also copy the top-level `import` statements from `decorators_module`. This should ensure the generated file supports everything contained in each file. However, you could end up with duplicated imports and aliases (e.g., having both `import numpy` and the other `import numpy as np`).
