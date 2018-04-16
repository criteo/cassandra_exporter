FROM openjdk:8u151-jre-alpine3.7

RUN apk update && apk add bash && \
    rm -rf /tmp/* /var/tmp/* /var/cache/apk/* /var/cache/distfiles/* && \
    mkdir -p /etc/cassandra_exporter /opt/cassandra_exporter

ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64 /sbin/dumb-init
ADD https://github.com/criteo/cassandra_exporter/releases/download/1.0.1/cassandra_exporter-1.0.1-all.jar /opt/cassandra_exporter/cassandra_exporter.jar
ADD config.yml /etc/cassandra_exporter/
ADD run.sh /

RUN chmod +x /sbin/dumb-init

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
