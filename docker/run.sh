#!/bin/bash

set -e

export CASSANDRA_EXPORTER_host="${CASSANDRA_EXPORTER_host:-localhost:7199}"
export CASSANDRA_EXPORTER_ssl="${CASSANDRA_EXPORTER_ssl:-False}"
export CASSANDRA_EXPORTER_user="${CASSANDRA_EXPORTER_user}"
export CASSANDRA_EXPORTER_password="${CASSANDRA_EXPORTER_password}"
export CASSANDRA_EXPORTER_listenPort="${CASSANDRA_EXPORTER_listenPort:-8080}"

echo "Starting Cassandra exporter"
while IFS='=' read -r name value ; do
  if [[ $name == 'CASSANDRA_EXPORTER_'* ]]; then
    val="${!name}"
    echo "$name $val"
    field=$(echo $name | sed -r 's/CASSANDRA_EXPORTER_(.+)/\1/')
    sed -ri "s/^($field):.*/\1: $val/" "/etc/cassandra_exporter/config.yml"
  fi
done < <(env)

/sbin/dumb-init /usr/bin/java -jar /opt/cassandra_exporter/cassandra_exporter.jar /etc/cassandra_exporter/config.yml
