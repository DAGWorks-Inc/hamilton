FROM python:3.13.1-slim

RUN apt update
RUN apt upgrade sqlite3 -y
RUN pip install "sf-hamilton[ui,sdk]"
RUN pip install flask

ENV HAMILTON_ALLOWED_HOSTS=".snowflakecomputing.app"
ENV SERVER_PORT=8001

COPY pipeline_endpoint.py /pipeline_endpoint.py
COPY my_functions.py /my_functions.py

ENTRYPOINT /bin/bash -c "(hamilton ui --base-dir /hamilton-basedir &) && python /pipeline_endpoint.py"
