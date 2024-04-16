FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./
RUN pip install -r requirements.txt

ENV HAMILTON_TELEMETRY_ENABLED=false

COPY app ./app

CMD ["app.lambda_handler.lambda_handler"]
