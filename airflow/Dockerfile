FROM apache/airflow:2.10.2

USER root

# Install Java 17 and OS dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=hadoop3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt/ \
 && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} /opt/spark \
 && chown -R airflow:root /opt/spark \
 && rm /tmp/spark.tgz
