import argparse
import os
from pathlib import Path


def main():
    import uvicorn
    from hamilton_experiments import server

    parser = argparse.ArgumentParser(prog="hamilton-experiments")
    parser.description = "Hamilton Experiment Server launcher"

    parser.add_argument(
        "path",
        metavar="path",
        type=str,
        nargs="?",
        help="Set HAMILTON_EXPERIMENTS_PATH environment variable",
    )

    parser.add_argument("--host", default="127.0.0.1", type=str, help="Bind to this address")

    parser.add_argument("--port", default=8123, type=int, help="Bind to this port")

    args = parser.parse_args()

    if args.path:
        if Path(args.path, "json_cache.dat").exists() is False:
            raise ValueError(f"Server failed to launch. No metadata cache found at {args.path}")

        os.environ["HAMILTON_EXPERIMENTS_PATH"] = args.path

    uvicorn.run(server.app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
