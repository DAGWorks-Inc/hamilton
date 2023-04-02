import os
from datetime import datetime
from typing import Any, Dict


def get_file_loading_metadata(path: str) -> Dict[str, Any]:
    """Gives metadata from loading a file.
    This includes:
    - the file size
    - the file path
    - the last modified time
    - the current time
    """
    return {
        "file_size": os.path.getsize(path),
        "file_path": path,
        "file_last_modified": os.path.getmtime(path),
        "file_loaded_at": datetime.now().utcnow().timestamp(),
    }
