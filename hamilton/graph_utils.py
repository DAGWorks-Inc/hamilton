import ast
import hashlib
import inspect
from types import ModuleType
from typing import Callable, List, Tuple, Union


def is_submodule(child: ModuleType, parent: ModuleType):
    return parent.__name__ in child.__name__


def find_functions(function_module: ModuleType) -> List[Tuple[str, Callable]]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the function module and grabs all function definitions.
    :return: list of tuples of (func_name, function).
    """

    def valid_fn(fn):
        return (
            inspect.isfunction(fn)
            and not fn.__name__.startswith("_")
            and is_submodule(inspect.getmodule(fn), function_module)
        )

    return [f for f in inspect.getmembers(function_module, predicate=valid_fn)]


def hash_source_code(source: Union[str, Callable], strip: bool = False) -> str:
    """Hashes the source code of a function (str).

    The `strip` parameter requires Python 3.9

    If strip, try to remove docs and comments from source code string. Since
    they don't impact function behavior, they shouldn't influence the hash.
    """
    if isinstance(source, Callable):
        source = inspect.getsource(source)

    source = source.strip()

    if strip:
        try:
            # could fail if source is indented code.
            # see `remove_docs_and_comments` docstring for details.
            source = remove_docs_and_comments(source)
        except Exception:
            pass

    return hashlib.sha256(source.encode()).hexdigest()


def remove_docs_and_comments(source: str) -> str:
    """Remove the docs and comments from a source code string.

    The use of `ast.unparse()` requires Python 3.9

    1. Parsing then unparsing the AST of the source code will
    create a code object and convert it back to a string. In the
    process, comments are stripped.

    2. walk the AST to check if first element after `def` is a
    docstring. If so, edit AST to skip the docstring

    NOTE. The ast parsing will fail if `source` has syntax errors. For the
    majority of cases this is caught upstream (e.g., by calling `import`).
    The foreseeable edge case is if `source` is the result of `inspect.getsource`
    on a nested function, method, or callable where `def` isn't at column 0.
    Standard usage of Hamilton requires users to define functions/nodes at the top
    level of a module, and therefore no issues should arise.
    """
    parsed = ast.parse(source)
    for node in ast.walk(parsed):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        if not len(node.body):
            continue

        # check if 1st node is a docstring
        if not isinstance(node.body[0], ast.Expr):
            continue

        if not hasattr(node.body[0], "value") or not isinstance(node.body[0].value, ast.Str):
            continue

        # skip docstring
        node.body = node.body[1:]

    return ast.unparse(parsed)
