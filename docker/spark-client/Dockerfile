# syntax=docker/dockerfile:1
ARG JAVA_BASE_IMAGE=eclipse-temurin:17.0.13_11-jdk-jammy
FROM ${JAVA_BASE_IMAGE}

ARG SPARK_VERSION=4.1.1
ARG SPARK_HADOOP_PROFILE=hadoop3
ARG SCALA_VERSION=2.13.17
ARG SBT_VERSION=1.10.7
ARG APACHE_SPARK_DIST_BASE_URL=https://archive.apache.org/dist/spark

ENV SPARK_HOME=/opt/spark \
    SCALA_VERSION=${SCALA_VERSION} \
    SBT_HOME=/opt/sbt \
    PATH="/opt/spark/bin:/opt/spark/sbin:/opt/sbt/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      coreutils \
      curl \
      gzip \
      procps \
      tar \
      tini \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    spark_package="spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_PROFILE}"; \
    curl -fL "${APACHE_SPARK_DIST_BASE_URL}/spark-${SPARK_VERSION}/${spark_package}.tgz" -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    mv "/opt/${spark_package}" "${SPARK_HOME}"; \
    rm /tmp/spark.tgz

RUN set -eux; \
    curl -fL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" -o /tmp/sbt.tgz; \
    tar -xzf /tmp/sbt.tgz -C /opt; \
    rm /tmp/sbt.tgz; \
    sbt -Dsbt.rootdir=true --allow-empty sbtVersion

RUN mkdir -p /workspace /opt/spark-events /opt/spark-warehouse /opt/spark-checkpoints /opt/spark/conf \
    && chmod -R 777 /workspace /opt/spark-events /opt/spark-warehouse /opt/spark-checkpoints

WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
