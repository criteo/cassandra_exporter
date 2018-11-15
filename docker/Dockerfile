FROM openjdk:11-jre-slim-sid

ARG EXPORTER_VERSION=2.1.0
ARG EXPORTER_SHA512=560612fdcc312df299474a3d8b08c38dc476012f97396c85568438df1f2d2d341833c887d6d5930d44cbdca4459c7cac71d6dea77e71c2caa485aecb04231268

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

RUN chmod +x /sbin/dumb-init && chmod g+wrx -R /opt/cassandra_exporter

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
