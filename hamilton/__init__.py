try:
    from .version import VERSION
except ImportError:
    from version import VERSION

STR_VERSION = ".".join([str(i) for i in VERSION])
