import os
import sys

import pytest

from hamilton import registry


def test_disable_autoload_then_reenable():
    os.environ[registry.HAMILTON_AUTOLOAD_ENV] = "0"
    # remove pandas from the loaded modules
    # pandas is both a Hamilton dependency and it has pandas_extensions.py
    pandas_modules = [module_name for module_name in sys.modules if "pandas" in module_name]
    for module_name in pandas_modules:
        sys.modules.pop(module_name)

    # pandas shouldn't be autoloaded
    registry.initialize()
    assert "pandas" not in sys.modules

    # pandas should be autoloaded
    registry.enable_autoload()
    registry.initialize()
    assert "pandas" in sys.modules


@pytest.mark.parametrize("entrypoint", ["config_disable_autoload", "config_enable_autoload"])
def test_command_entrypoints_arent_renamed(entrypoint: str):
    """Ensures that functions associated with an entrypoint in
    pyproject.toml aren't renamed.

    This doesn't prevent the entrypoints from being renamed
    """
    assert hasattr(registry, entrypoint)
