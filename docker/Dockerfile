FROM eclipse-temurin:11-jre-jammy

ARG EXPORTER_VERSION=2.3.8

RUN apt-get update && apt-get install -y --no-install-recommends \
        dumb-init \
        netcat \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}.jar /opt/cassandra_exporter/cassandra_exporter.jar
ADD config.yml /etc/cassandra_exporter/
ADD docker/run.sh /

RUN chmod g+wrX,o+rX -R /opt/cassandra_exporter \
 && chmod g+wrX,o+rX -R /etc/cassandra_exporter

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "/run.sh"]
