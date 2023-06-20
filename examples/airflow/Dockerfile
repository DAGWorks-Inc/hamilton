FROM apache/airflow:latest-python3.10
ADD requirements.txt .
RUN pip install -r requirements.txt
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends graphviz \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
