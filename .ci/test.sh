#!/bin/bash

set -e -u -o pipefail

echo "running CI task '${TASK}'"

if [[ ${TASK} == "pre-commit" ]]; then
    pip install pre-commit
    pre-commit run --all-files
    exit 0
fi

echo "using venv at '${HOME}/venvs/hamilton-venv/bin/activate'"
source "${HOME}/venvs/hamilton-venv/bin/activate"

if [[ ${TASK} == "async" ]]; then
    pip install .
    pytest plugin_tests/h_async
    exit 0
fi

if [[ ${TASK} == "dask" ]]; then
    pip install -e '.[dask]'
    pytest plugin_tests/h_dask
    exit 0
fi

if [[ ${TASK} == "integrations" ]]; then
    pip install -e '.[pandera, test]'
    pip install -r tests/integrations/pandera/requirements.txt
    if python -c 'import sys; exit(0) if sys.version_info[:2] == (3, 9) else exit(1)'; then
      echo "Python version is 3.9"
      pip install dask-expr
    else
      echo "Python version is not 3.9"
    fi
    pytest tests/integrations
    exit 0
fi

if [[ ${TASK} == "ray" ]]; then
    pip install -e '.[ray]'
    pytest plugin_tests/h_ray
    exit 0
fi

if [[ ${TASK} == "pyspark" ]]; then
    pip install -e '.[pyspark]'
    pip install 'numpy<2' 'pyspark[connect]' # downgrade until spark fixes their bug
    pytest plugin_tests/h_spark
    exit 0
fi

if [[ ${TASK} == "vaex" ]]; then
    pip install "numpy<2.0.0"  # numpy2.0 breaks vaex
    pip install -e '.[vaex]'
    pytest plugin_tests/h_vaex
    exit 0
fi

if [[ ${TASK} == "narwhals" ]]; then
    pip install -e .
    pip install polars pandas narwhals
    pytest plugin_tests/h_narwhals
    exit 0
fi

if [[ ${TASK} == "tests" ]]; then
    pip install .
    # https://github.com/plotly/Kaleido/issues/226
    pip install "kaleido<0.4.0" # kaleido 0.4.0 breaks plotly; TODO: remove this
    pytest \
        --cov=hamilton \
        --ignore tests/integrations \
        tests/
    exit 0
fi

echo "ERROR: did not recognize TASK '${TASK}'"
exit 1
