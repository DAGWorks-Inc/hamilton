
Installing hamilton is easy!

# Installing

Hamilton is a lightweight framework with a variety of extensions/plugins. To get started, you'll need the following:

- ``python >= 3.7``
- ``pip``

For help with python/pip/managing virtual environments see the [python docs](https://docs.python.org/3/tutorial/venv.html/).

## Installing with pip

Hamilton is published on [pypi](https://pypi.org/project/sf-hamilton/) under ``sf-hamilton``. To install, run:

`pip install sf-hamilton`

To use the DAG visualization functionality, instead install with

`pip install sf-hamilton[visualization]`

*Note: for visualization you may additionally need to install graphviz externally -- see*
[graphviz](https://graphviz.org/download/) *for instructions on the correct way for your
operating system.*

## Installing with conda

Hamilton is also available on conda if you prefer:

`conda install -c hamilton-opensource sf-hamilton`

## Installing from source


You can also download the code and run it from the source.

```bash
git clone https://github.com/dagworks-inc/hamilton.git
cd hamilton
pip install -e .
```
