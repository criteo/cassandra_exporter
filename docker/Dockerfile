FROM openjdk:10.0.1-10-jre-slim-sid

ARG EXPORTER_VERSION=1.0.1
ARG EXPORTER_SHA512=722e7f646948ad20ad801dcc38c6eff1f3295b5e412013c0b5164e342aed1557de439af0b042deaa006306dceb13d939e99076fd397eddc2401ebfd0dfe4555b

RUN mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
ADD https://github.com/criteo/cassandra_exporter/releases/download/${EXPORTER_VERSION}/cassandra_exporter-${EXPORTER_VERSION}-all.jar /opt/cassandra_exporter/cassandra_exporter.jar
RUN echo "${EXPORTER_SHA512}  /opt/cassandra_exporter/cassandra_exporter.jar" > sha512_checksum.txt && sha512sum -c sha512_checksum.txt
ADD config.yml /etc/cassandra_exporter/
ADD run.sh /

RUN chmod +x /sbin/dumb-init

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
