FROM openjdk:11-jre-slim-sid

ARG EXPORTER_VERSION=2.2.0
ARG EXPORTER_SHA512=071139ab686fe98bb940c41485d6ce25b50781e0c4795fc9d006d6d5081f398c6c17a9cebf11d17c5f03873ad915252278149787da794b42d85082d0ba6c69a4

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

RUN chmod +x /sbin/dumb-init && chmod g+wrx -R /opt/cassandra_exporter && chmod g+wrx -R /etc/cassandra_exporter

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
