FROM bitnami/spark:latest

USER root

# Install AWS SDKs for S3 + DynamoDB interaction
RUN pip install boto3


WORKDIR /app

COPY transform.py .

