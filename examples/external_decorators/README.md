# External decorators demo

This example is in response to this [GitHub issue](https://github.com/DAGWorks-Inc/hamilton/issues/50). However, it showcases more general mechanisms of how to parse "Hamilton  modules" and dynamically alter source code.

# Objective
Use an existing Python module that has no Python dependencies and decorated it with Hamilton functionalities without altering the source file.

# Content
The script run.py will:
1. load the module `my_functions.py`, remove decorators and hamilton imports and generate `my_functions_no_hamilton.py`
2. load the module `external_decorators.py`, which defines decorated empty functions and map them onto `my_functions_no_hamilton.py`
3. store the dynamically created `newly_decorated.py` or load it unto a temporary Python module (never needs file writing access)

# Methods
- By using the abstract syntax tree (how Python represents the text as grammar), the files can be edited programmatically and attributes transferred between them
- The example also shows how to load text as a Python module without needing writing access

# Notes and limitations
- You will probably want to run your linter, debugger, etc. on a generated file
- This approach makes debugging and version control harder because of multiple files being involved. On the other hand, if you really cannot change your original file, it can provide a more robust sync between it and a copy that has Hamilton decorators
- The Python importlib, ast, types, typing are areas of active development and may change in the future. Python version control is important if your system depends on these features.
