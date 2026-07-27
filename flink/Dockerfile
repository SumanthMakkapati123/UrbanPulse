FROM flink:2.1.1-scala_2.12-java17

USER root
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        build-essential curl openjdk-17-jdk-headless python3 python3-dev python3-pip \
    && jdk_home="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")" \
    && cp -a "$jdk_home/include" /opt/java/openjdk/include \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python
RUN pip3 install --no-cache-dir apache-flink==2.1.1
RUN curl --fail --location --silent --show-error \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar \
    --output /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.1.jar
USER flink
