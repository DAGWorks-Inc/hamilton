try:
    from .version import VERSION as __version__  # noqa: F401
except ImportError:
    from version import VERSION as __version__  # noqa: F401
