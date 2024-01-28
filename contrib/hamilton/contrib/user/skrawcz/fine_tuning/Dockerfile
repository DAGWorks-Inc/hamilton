FROM python:3.10-slim-bullseye

WORKDIR /app

# install graphviz backend
RUN apt-get update \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY example-support-dataset.json .

COPY . .

EXPOSE 8080:8080

# run the module
CMD python __init__.py
