import inspect
from types import ModuleType
from typing import Callable, List, Tuple


def is_submodule(child: str, parent: str):
    return parent in child


def find_functions(function_module: ModuleType) -> List[Tuple[str, Callable]]:
    """Function to determine the set of functions we want to build a graph from.

    This iterates through the function module and grabs all function definitions.
    :return: list of tuples of (func_name, function).
    """

    def valid_fn(fn):
        return (
            inspect.isfunction(fn)
            and not fn.__name__.startswith("_")
            and is_submodule(fn.__module__, function_module.__name__)
        )

    return [f for f in inspect.getmembers(function_module, predicate=valid_fn)]
