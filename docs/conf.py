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
    "announcement": '📢 Next Hamilton meet-up: <a target="_blank" href="https://www.meetup.com/agile-data-london/events/293589063/">'
    + "May 31st - London 🇬🇧. Spots limited. Sign up!</a>📢",
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
]
