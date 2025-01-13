import inspect
from types import ModuleType
from typing import Callable, List, Tuple


def is_submodule(child: ModuleType, parent: ModuleType):
    return parent.__name__ in child.__name__


def is_hamilton_function(fn: Callable) -> bool:
    """A `Hamilton function` defines a node.
    To be valid, it must not start with the underscore `_` prefix.
    """
    return inspect.isfunction(fn) and not fn.__name__.startswith("_")


# NOTE. This should return a list of callables instead of tuples. Internally,
# the function names are never used (except in the CLI) and that information
# is readily available through `fn.__name__`.
# Care is required because users may have been advised to use this code path.
def find_functions(function_module: ModuleType) -> List[Tuple[str, Callable]]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the function module and grabs all function definitions.
    :return: list of tuples of (func_name, function).
    """
    return [
        (name, fn)
        for name, fn in inspect.getmembers(function_module)
        if is_hamilton_function(fn) and is_submodule(inspect.getmodule(fn), function_module)
    ]
