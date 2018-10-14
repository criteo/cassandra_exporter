FROM openjdk:11-jre-slim-sid

ARG EXPORTER_VERSION=2.0.3
ARG EXPORTER_SHA512=ce034503ab0f5239cdb1e1e6f6c3b47109899fdb65f1c0bdb62d3bed25aabe6978315a7e3d20674b8d72102f9c078370bae4af03a0ae9bdf44064a416578810d

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

RUN chmod +x /sbin/dumb-init

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
