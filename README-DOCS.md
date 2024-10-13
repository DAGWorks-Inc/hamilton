# Documentation

Instructions for managing documentation on read the docs.

# Build locally

To build locally, you need to run the following -- make sure you're in the root of the repo:

```bash
pip install .[docs]
```
and then one of the following to build and view the documents:
```bash
sphinx-build -b dirhtml -W -E -T -a docs /tmp/mydocs
python -m http.server --directory /tmp/mydocs
```
or for auto rebuilding do:
```bash
sphinx-autobuild -b dirhtml -W -E -T  --watch hamilton/ -a docs /tmp/mydocs
```
Then it'll be running on port 8000.

Note: readthedocs builds will fail if there are ANY WARNINGs in the build.
So make sure to check the build log for any warnings, and fix them, else you'll waste time debugging readthedocs
build failures.

# SimplePDF
To create a PDF, you can run the following:
```bash
sphinx-build -b simplepdf  -W -E -T  -a docs /tmp/mydocs
# or if you want to auto-rebuild:
sphinx-autobuild -b simplepdf  -W -E -T  --watch hamilton/ -a docs /tmp/mydocs
```
The PDF will be in `/tmp/mydocs` in a few minutes.

# reST vs myST
We use both! The general breakdown of when to use which is:
1. For documentation that we want to be viewable in github, use myST.
2. Otherwise default to using reST.
