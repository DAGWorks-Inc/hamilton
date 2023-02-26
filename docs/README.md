# Documentation

Instructions for managing documentation on read the docs

# Build locally

To build locally, you need to run the following:

pip install -r docs/requirements.txt
pip install furo
sphinx-build -a docs /tmp/mydocs
python -m http.server --directory /tmp/mydocs

Then it'll be running on port 8000
