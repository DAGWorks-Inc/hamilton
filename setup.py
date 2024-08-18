#!/usr/bin/env python
"""The setup script."""

import warnings

from setuptools import setup

# don't fail if there are problems with the readme (happens within circleci)
try:
    with open("README.md") as readme_file:
        readme = readme_file.read()
except Exception:
    warnings.warn("README.md not found")  # noqa
    readme = None


def get_version():
    version_dict = {}
    with open("hamilton/version.py") as f:
        exec(f.read(), version_dict)
    return ".".join(map(str, version_dict["VERSION"]))


VERSION = get_version()


setup(
    version=VERSION,
    long_description=readme,
    long_description_content_type="text/markdown",
    zip_safe=False,
)
