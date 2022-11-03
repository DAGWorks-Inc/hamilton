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
    pytest graph_adapter_tests/h_async
    exit 0
fi

if [[ ${TASK} == "dask" ]]; then
    pip install -e '.[dask]'
    pytest graph_adapter_tests/h_dask
    exit 0
fi

if [[ ${TASK} == "integrations" ]]; then
    pip install -e '.[pandera]'
    pytest tests/integrations
    exit 0
fi

if [[ ${TASK} == "ray" ]]; then
    pip install -e '.[ray]'
    pytest graph_adapter_tests/h_ray
    exit 0
fi

if [[ ${TASK} == "pyspark" ]]; then
    pip install -e '.[pyspark]'
    pytest graph_adapter_tests/h_spark
    exit 0
fi

if [[ ${TASK} == "tests" ]]; then
    pip install .
    pytest \
        --cov=hamilton \
        --ignore tests/integrations \
        tests/
    exit 0
fi

echo "ERROR: did not recognize TASK '${TASK}'"
exit 1
