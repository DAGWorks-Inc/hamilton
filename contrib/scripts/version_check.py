import requests
from packaging import version

# Assume CURRENT_VERSION is extracted from your package file
from hamilton.contrib import version as contrib_version

CURRENT_VERSION = ".".join(map(str, contrib_version.VERSION))

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
