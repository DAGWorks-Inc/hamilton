import argparse
import os
from pathlib import Path

from hamilton.plugins.h_experiments.cache import JsonCache


def main():
    try:
        import fastapi  # noqa: F401
        import fastui  # noqa: F401
        import uvicorn
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            "Some dependencies are missing. Make sure to `pip install sf-hamilton[experiments]`"
        )

    parser = argparse.ArgumentParser(prog="hamilton-experiments")
    parser.description = "Hamilton Experiment Server launcher"

    parser.add_argument(
        "path",
        metavar="path",
        type=str,
        default="./experiments",
        nargs="?",
        help="Set HAMILTON_EXPERIMENTS_PATH environment variable",
    )
    parser.add_argument("--host", default="127.0.0.1", type=str, help="Bind to this address")
    parser.add_argument("--port", default=8123, type=int, help="Bind to this port")

    args = parser.parse_args()

    try:
        JsonCache(args.path)
    except Exception as e:
        raise ValueError(f"Server failed to launch. No metadata cache found at {args.path}") from e

    # set environment variable that FastAPI will use
    os.environ["HAMILTON_EXPERIMENTS_PATH"] = str(Path(args.path).resolve())

    uvicorn.run("hamilton.plugins.h_experiments.server:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
