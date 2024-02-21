import json
import subprocess


def main():
    # equivalent to using your command line with
    # `hamilton --verbose --json-out version ./module_v1.py`
    result = subprocess.run(
        ["hamilton", "--verbose", "--json-out", "version", "./module_v1.py"],
        stdout=subprocess.PIPE,
        text=True,
    )

    # `--json-out` outputs the result as JSON string on a single line
    # `--verbose` outputs intermediary commands on separate lines
    responses = []
    for line in result.stdout.splitlines():
        command_response = json.loads(line)
        responses.append(command_response)

    print(responses)


if __name__ == "__main__":
    main()
