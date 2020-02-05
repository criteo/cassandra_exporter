FROM openjdk:11-jre-slim-sid

ARG EXPORTER_VERSION=2.2.1
ARG EXPORTER_SHA512=ef83fc1404fce6c6b949a9dfe0f1f0bba9b2d21591550b954439723500e0bc17ee083d9995cc212d646e519614fef0f7c6802ce2635319bdd3ca69b0c51e91ad

RUN apt-get update && apt-get install -y --no-install-recommends \
		netcat \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}-all.jar /opt/cassandra_exporter/cassandra_exporter.jar
RUN echo "${EXPORTER_SHA512}  /opt/cassandra_exporter/cassandra_exporter.jar" > sha512_checksum.txt && sha512sum -c sha512_checksum.txt
ADD config.yml /etc/cassandra_exporter/
ADD run.sh /

RUN chmod +x /sbin/dumb-init && chmod g+wrX,o+rX -R /opt/cassandra_exporter && chmod g+wrX,o+rX -R /etc/cassandra_exporter

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
