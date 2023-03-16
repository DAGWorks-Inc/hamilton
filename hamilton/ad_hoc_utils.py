"""A suite of tools for ad-hoc use"""
import sys
import types
import uuid
from types import ModuleType
from typing import Callable


def _copy_func(f):
    """Returns a function with the same properties as the original one"""
    fn = types.FunctionType(f.__code__, f.__globals__, f.__name__, f.__defaults__, f.__closure__)
    # in case f was given attrs (note this dict is a shallow copy):
    fn.__dict__.update(f.__dict__)
    fn.__annotations__ = f.__annotations__  # No idea why this is not a parameter...
    return fn


def _generate_unique_temp_module_name() -> str:
    """Generates a unique module name that is a valid python variable."""
    return f"temporary_module_{str(uuid.uuid4()).replace('-', '_')}"


def create_temporary_module(*functions: Callable, module_name: str = None) -> ModuleType:
    """Creates a temporary module usable by hamilton. Note this should *not* be used in production --
    you should really be organizing your functions into modules. This is perfect in a jupyter notebook,
    however, where you have a few functions that you want to string together to build an ad-hoc driver. See notes below:

    NOTE (1): this currently *DOES NOT WORK* for scaling out onto Ray, Dask, or Pandas on Spark.

    NOTE (2): that this is slightly dangerous -- we want the module to look and feel like an actual module
    so we can fully duck-type it. We thus stick it in sys.modules (checking if it already exists).

    :param functions: Functions to use
    :param module_name: Module name to use. If not provided will default to a unique one.
    :return: a "module" housing the passed in functions
    """
    module_name = module_name if module_name is not None else _generate_unique_temp_module_name()
    if module_name in sys.modules:
        raise ValueError(f"Module already exists with name: {module_name}, please make it unique.")
    module = ModuleType(module_name)
    for fn in map(_copy_func, functions):  # Copies so we don't mess with the original functions
        fn_name = fn.__name__
        if hasattr(module, fn_name):
            raise ValueError(
                f"Duplicate/reserved function name: {fn_name} cannot be used to create a temporary module."
            )
        fn.__module__ = module.__name__
        setattr(module, fn_name, fn)
    sys.modules[module_name] = module
    return module
