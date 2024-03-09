import importlib
from typing import Callable
from types import ModuleType

from hamilton import telemetry
from hamilton.ad_hoc_utils import _copy_func

import pytest


telemetry.disable_telemetry()


@pytest.fixture
def override_module_functions() -> Callable[[ModuleType, list[Callable]], ModuleType]:
    """Fixture returning function to override module functions"""
    def _override_module_functions(
        module: ModuleType, functions: list[Callable]
    ) -> ModuleType:
        """Utility to override module functions with passed function"""
        spec: importlib.machinery.ModuleSpec = module.__spec__
        module_copy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module_copy)

        for fn in map(_copy_func, functions):
            fn_name = fn.__name__
            fn.__module__ = module_copy.__name__
            setattr(module_copy, fn_name, fn)

        return module_copy
    
    return _override_module_functions
