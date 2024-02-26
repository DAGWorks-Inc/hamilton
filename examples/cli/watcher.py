import sys

from watchdog.events import FileModifiedEvent
from watchdog.observers import Observer
from watchdog.tricks import ShellCommandTrick

if __name__ == "__main__":
    cmd = sys.argv[1:]

    event_handler = ShellCommandTrick(
        shell_command=" ".join(cmd),
        patterns=["*.py"],
    )
    observer = Observer()
    observer.schedule(
        event_handler,
        path=".",
        event_filter=[FileModifiedEvent],
    )
    observer.start()
    print(f"Watching with {cmd}")
    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()
