# Using the official Jupyter PySpark image
FROM jupyter/pyspark-notebook:latest

USER root

# Install helpful linux tools
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Pre-download Hadoop-AWS connector jars
# These versions depend on the Spark/Hadoop version in the base image.
# For current 'latest' (Spark 3.5+ / Hadoop 3.3+), these are standard:
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN curl -o /usr/local/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -o /usr/local/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Set the working directory
WORKDIR /home/jovyan/notebooks

USER jovyan