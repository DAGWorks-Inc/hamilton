#!/bin/bash

set -e -u -o pipefail

OPERATING_SYSTEM=$(uname -s)

if [[ ${OPERATING_SYSTEM} == "Linux" ]]; then
    sudo apt-get update -y
    sudo apt-get install \
        --no-install-recommends \
        --yes \
            graphviz
fi

# setting up a virtualenv isn't necessary for the "pre-commit" task
if [[ ${TASK} != "pre-commit" ]]; then
    mkdir -p "${HOME}/venvs/hamilton-venv"
    python -m venv "${HOME}/venvs/hamilton-venv"
    source "${HOME}/venvs/hamilton-venv/bin/activate"
    pip install \
        -r requirements-test.txt
fi

if [[ ${TASK} == "async" ]]; then
    pip install \
        -r graph_adapter_tests/h_async/requirements-test.txt
fi

if [[ ${TASK} == "pyspark" ]]; then
    if [[ ${OPERATING_SYSTEM} == "Linux" ]]; then
        sudo apt-get install \
            --no-install-recommends \
            --yes \
                default-jre
    fi
fi

echo "----- python version -----"
python --version

echo "----- pip version -----"
pip --version
echo "-----------------------"
