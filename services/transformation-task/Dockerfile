FROM bitnami/spark:3.5

USER root



# Set working directory
WORKDIR /opt/spark

RUN apt-get update && apt-get install -y curl
RUN pip install boto3 delta-spark

# Create folder for Spark JARs
RUN mkdir -p /opt/spark/jars

# Use curl to download the Delta Lake JAR
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar
# # Download Hadoop AWS JAR (for S3 support)
# RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# # Download AWS Java SDK bundle JAR (required by hadoop-aws)
# RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Set environment variable so Spark can use it
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*


COPY transform.py .


CMD ["spark-submit", "--master", "local", "transform.py"]
