#!/bin/bash

set -e

export JVM_OPTS="${JVM_OPTS:-Xmx}"
export CASSANDRA_EXPORTER_CONFIG_host="${CASSANDRA_EXPORTER_CONFIG_host:-localhost:7199}"
export CASSANDRA_EXPORTER_CONFIG_ssl="${CASSANDRA_EXPORTER_CONFIG_ssl:-False}"
export CASSANDRA_EXPORTER_CONFIG_user="${CASSANDRA_EXPORTER_CONFIG_user}"
export CASSANDRA_EXPORTER_CONFIG_password="${CASSANDRA_EXPORTER_CONFIG_password}"
export CASSANDRA_EXPORTER_CONFIG_listenPort="${CASSANDRA_EXPORTER_CONFIG_listenPort:-8080}"

echo "Starting Cassandra exporter"
echo "JVM_OPTS: $JVM_OPTS"
while IFS='=' read -r name value ; do
  if [[ $name == 'CASSANDRA_EXPORTER_CONFIG_'* ]]; then
    val="${!name}"
    echo "$name $val"
    field=$(echo $name | sed -r 's/CASSANDRA_EXPORTER_CONFIG_(.+)/\1/')
    sed -ri "s/^($field):.*/\1: $val/" "/etc/cassandra_exporter/config.yml"
  fi
done < <(env)

/sbin/dumb-init /usr/bin/java ${JVM_OPTS} -jar /opt/cassandra_exporter/cassandra_exporter.jar /etc/cassandra_exporter/config.yml
