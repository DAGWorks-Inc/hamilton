import logging
from contextlib import contextmanager

try:
    from .version import VERSION as __version__  # noqa: F401
except ImportError:
    from version import VERSION as __version__  # noqa: F401

from hamilton import telemetry


def track(module_name: str):
    """Function to call to track module usage."""
    if hasattr(telemetry, "create_and_send_contrib_use"):  # makes sure Hamilton version is fine.
        telemetry.create_and_send_contrib_use(module_name, __version__)


@contextmanager
def catch_import_errors(module_name: str, file_location: str, logger: logging.Logger):
    try:
        # Yield control to the inner block which will have the import statements.
        yield
        # After all imports succeed send telemetry
        track(module_name)
    except ImportError as e:
        location = file_location[: file_location.rfind("/")]
        logger.error("ImportError: %s", e)
        logger.error(
            "Please install the required packages. Options:\n"
            f"(1): with `pip install -r {location}/requirements.txt`\n"
        )
        raise e
