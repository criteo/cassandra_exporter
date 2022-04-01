FROM openjdk:11-jre-slim-buster

ARG EXPORTER_VERSION=2.3.8

RUN apt-get update && apt-get install -y --no-install-recommends \
		netcat \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
RUN chmod +x /sbin/dumb-init

run mkdir -p /opt/cassandra_exporter
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}.jar /opt/cassandra_exporter/cassandra_exporter.jar

RUN chmod g+wrX,o+rX -R /opt/cassandra_exporter

RUN mkdir -p /etc/cassandra_exporter
RUN chmod g+wrX,o+rwX -R /etc/cassandra_exporter

USER 1000

ADD config.yml /etc/cassandra_exporter/
ADD run.sh /

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
