FROM python:3.11-slim-buster

RUN apt-get update && apt-get install -y graphviz

COPY requirements.txt ./

RUN pip install -r requirements.txt

ENV HAMILTON_TELEMETRY_ENABLED=false

ENTRYPOINT ["python3"]
