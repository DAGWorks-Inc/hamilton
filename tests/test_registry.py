import pytest

from hamilton import registry


@pytest.mark.parametrize("entrypoint", ["config_disable_autoload", "config_enable_autoload"])
def test_command_entrypoints_arent_renamed(entrypoint: str):
    """Ensures that functions associated with an entrypoint in
    pyproject.toml aren't renamed.

    This doesn't prevent the entrypoints from being renamed
    """
    assert hasattr(registry, entrypoint)
