import os
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
}
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_parser",
]
