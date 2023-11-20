import requests
from packaging import version

VERSION = None
with open("hamilton/contrib/version.py", "r") as f:
    lines = f.readlines()
    for line in lines:
        if line.startswith("VERSION"):
            exec(line)  # this will set VERSION to the correct tuple.

CURRENT_VERSION = ".".join(map(str, VERSION))

# Replace 'your-package-name' with your actual package name on PyPI
response = requests.get("https://pypi.org/pypi/sf-hamilton-contrib/json")
data = response.json()
pypi_version = data["info"]["version"]

if version.parse(CURRENT_VERSION) > version.parse(pypi_version):
    print("New version is greater!")
    is_greater = "true"
else:
    print("Current version is not greater than the published version.")
    is_greater = "false"

# This would be the actual line in the script
print(f"::set-output name=version_is_greater::{is_greater}")
