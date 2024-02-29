# Documentation

Instructions for managing documentation on read the docs.

# Build locally

To build locally, you need to run the following -- make sure you're in the root of the repo:

```bash
pip install -r requirements-docs.txt
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

# reST vs myST
We use both! The general breakdown of when to use which is:
1. For documentation that we want to be viewable in github, use myST.
2. Otherwise default to using reST.
