import os
from datetime import datetime
from typing import Any, Dict


def get_file_metadata(path: str) -> Dict[str, Any]:
    """Gives metadata from loading a file.
    This includes:
    - the file size
    - the file path
    - the last modified time
    - the current time
    """
    return {
        "size": os.path.getsize(path),
        "path": path,
        "last_modified": os.path.getmtime(path),
        "timestamp": datetime.now().utcnow().timestamp(),
    }
