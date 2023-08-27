import os
import re
import subprocess
import sys

# required to get reference documentation to be built
sys.path.insert(0, os.path.abspath(".."))

project = "Hamilton"

html_theme = "furo"
html_title = "Hamilton"
html_theme_options = {
    "source_repository": "https://github.com/dagworks-inc/hamilton",
    "source_branch": "main",
    "source_directory": "docs/",
    "announcement": "📢 Find Hamilton next at "
    + '<a target="_blank" href="https://www.featurestoresummit.com/">Feature Store Summit\'23</a>📢',
    "light_css_variables": {
        "color-announcement-background": "#ffba00",
        "color-announcement-text": "#091E42",
    },
    "dark_css_variables": {
        "color-announcement-background": "#ffba00",
        "color-announcement-text": "#091E42",
    },
}
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_parser",
    "sphinx_sitemap",
    "docs.data_adapters_extension",
]

# for the sitemap extension ---
# check if the current commit is tagged as a release (vX.Y.Z) and set the version
GIT_TAG_OUTPUT = subprocess.check_output(["git", "tag", "--points-at", "HEAD"])
current_tag = GIT_TAG_OUTPUT.decode().strip()
if re.match(r"^sf-hamilton-(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$", current_tag):
    version = current_tag
else:
    version = "latest"

html_baseurl = "https://hamilton.dagworks.io/"
html_extra_path = ["robots.txt"]
# ---
