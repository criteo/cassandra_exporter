FROM openjdk:11-jre-slim-buster

ARG EXPORTER_VERSION=2.3.7

RUN apt-get update && apt-get install -y --no-install-recommends \
		netcat \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}.jar /opt/cassandra_exporter/cassandra_exporter.jar
ADD config.yml /etc/cassandra_exporter/
ADD docker/run.sh /

RUN chmod +x /sbin/dumb-init && chmod g+wrX,o+rX -R /opt/cassandra_exporter && chmod g+wrX,o+rX -R /etc/cassandra_exporter

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
