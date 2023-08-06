FROM python:3.10-slim-bullseye

WORKDIR /app

# install graphviz backend
RUN apt-get update \
    && apt-get install -y --no-install-recommends graphviz \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080:8080

# loads server.py module and the FastAPI `app` object
CMD uvicorn server:app --host 0.0.0.0 --port 8080
