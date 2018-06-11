FROM openjdk:10.0.1-10-jre-slim-sid

ARG EXPORTER_VERSION=2.0.0
ARG EXPORTER_SHA512=7baa4e13f0a3c4228ba9d6cb848027f8837de0a0bc2a6b4fc3d8265e00a53fe926a6eac75a32a84de5e0771b355c1a8715dd46886c134710c6f26f477010b9d3

RUN mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}-all.jar /opt/cassandra_exporter/cassandra_exporter.jar
RUN echo "${EXPORTER_SHA512}  /opt/cassandra_exporter/cassandra_exporter.jar" > sha512_checksum.txt && sha512sum -c sha512_checksum.txt
ADD config.yml /etc/cassandra_exporter/
ADD run.sh /

RUN chmod +x /sbin/dumb-init

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
