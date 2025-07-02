FROM bitnami/spark:3.5

USER root

# Install AWS SDKs for S3 + DynamoDB interaction
RUN pip install boto3


WORKDIR /app

COPY transform.py .

