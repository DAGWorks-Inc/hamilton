import ast
import hashlib
import inspect
from types import ModuleType
from typing import Callable, List, Tuple


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


def hash_callable(node_callable: Callable) -> str:
    """Create a single hash (str) from the bytecode of all sorted functions"""
    source = inspect.getsource(node_callable)
    return hashlib.sha256(source.encode()).hexdigest()


def hash_source_code(source: str | Callable, strip: bool = False) -> str:
    """Create a single hash (str) from the bytecode of a function"""
    if isinstance(source, Callable):
        source = inspect.getsource(source)
        
    if strip:
        source = remove_docs_and_comments(source)
    
    return hashlib.sha256(source.encode()).hexdigest()


def remove_docs_and_comments(source: str):
    # TODO unparse will fail if inspect.getsource returned nested functions
    # because these functions are indented (don't start a column 0)
    # a hacky solution is to prepend `if True:` at the top
    source = "if True:\n" + source
    comments_stripped = ast.unparse(ast.parse(source))
    
    formatted_code = ""
    for line in comments_stripped.split("\n"):
        if line == "if True:\n":
            continue
        
        if line.lstrip()[:1] in ("'", '"'):
            continue
        
        formatted_code += line + "\n"
    
    return formatted_code 
