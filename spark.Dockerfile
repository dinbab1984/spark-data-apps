FROM ibmjava:jre

# -- Layer: Apache Spark

ARG spark_version=3.4.1
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark-dist.tgz && \
    tar -xf spark-dist.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version}/ /opt/spark && \
    mkdir /opt/spark/logs && \
    rm spark-dist.tgz

ENV SPARK_HOME /opt/spark
ENV SPARK_MASTER_HOST 0.0.0.0
ENV SPARK_MASTER_PORT 7077

WORKDIR ${SPARK_HOME}