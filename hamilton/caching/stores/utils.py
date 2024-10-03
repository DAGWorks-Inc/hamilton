import pathlib


def get_directory_size(directory: str) -> float:
    total_size = 0
    for p in pathlib.Path(directory).rglob("*"):
        if p.is_file():
            total_size += p.stat().st_size

    return total_size


def readable_bytes_size(n_bytes: float) -> str:
    labels = ["B", "KB", "MB", "GB", "TB"]
    exponent = 0

    while n_bytes > 1024.0:
        n_bytes /= 1024.0
        exponent += 1

    return f"{n_bytes:.2f} {labels[exponent]}"
