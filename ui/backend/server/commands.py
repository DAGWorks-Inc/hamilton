import logging
import os
import sys
import threading
import time
import webbrowser
from contextlib import contextmanager

import hamilton_ui
import requests
from django.core.management import execute_from_command_line

logger = logging.getLogger(__name__)


@contextmanager
def extend_sys_path(path):
    original_sys_path = sys.path[:]
    try:
        sys.path.insert(0, path)
        yield
    finally:
        sys.path = original_sys_path


# Usage

# Perform operations that require importing modules from the specified path


@contextmanager
def set_env_variables(vars: dict):
    original_values = {}
    try:
        # Set new values and store original values
        for key, value in vars.items():
            original_values[key] = os.getenv(key)
            os.environ[key] = value
        yield
    finally:
        # Restore original values or delete if they were not set originally
        for key in vars:
            if original_values[key] is None:
                del os.environ[key]
            else:
                os.environ[key] = original_values[key]


def _open_when_ready(check_url: str, open_url: str):
    while True:
        try:
            response = requests.get(check_url)
            if response.status_code == 200:
                webbrowser.open(open_url)
                return
            else:
                pass
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)


def run(port: int, base_dir: str, no_migration: bool, no_open: bool):
    env = {
        "HAMILTON_BASE_DIR": base_dir,
    }
    if not no_open:
        thread = threading.Thread(
            target=_open_when_ready,
            kwargs={
                "open_url": (open_url := f"http://localhost:{port}"),
                "check_url": f"{open_url}/api/v0/health",
            },
            daemon=True,
        )
        thread.start()
    with set_env_variables(env):
        with extend_sys_path(hamilton_ui.__path__[0]):
            # This is here as some folks didn't have it created automatically through django
            if not os.path.exists(base_dir):
                try:
                    os.makedirs(os.path.join(base_dir, "blobs"))
                    os.makedirs(os.path.join(base_dir, "db"))
                except Exception as e:
                    logger.exception(
                        f"Error creating directories -- manually create them instead: {e}"
                    )
            if not no_migration:
                execute_from_command_line(
                    ["manage.py", "migrate", "--settings=server.settings_mini"]
                )
            execute_from_command_line(
                ["manage.py", "runserver", "--settings=server.settings_mini", f"0.0.0.0:{port}"]
            )
