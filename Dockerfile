FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN apt update && \
    pip install google-api-python-client google-cloud-secret-manager && \
    pip install -r requirements.txt

CMD scripts/gcr_serve.py scripts/daily.sh