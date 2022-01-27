#!/bin/bash

set -e

export JVM_OPTS="${JVM_OPTS}" # i.e -Xmx1024m
#export CASSANDRA_EXPORTER_CONFIG_host="${CASSANDRA_EXPORTER_CONFIG_host:-localhost:7199}"
#export CASSANDRA_EXPORTER_CONFIG_ssl="${CASSANDRA_EXPORTER_CONFIG_ssl:-False}"
#export CASSANDRA_EXPORTER_CONFIG_user="${CASSANDRA_EXPORTER_CONFIG_user}"
#export CASSANDRA_EXPORTER_CONFIG_password="${CASSANDRA_EXPORTER_CONFIG_password}"
#export CASSANDRA_EXPORTER_CONFIG_listenPort="${CASSANDRA_EXPORTER_CONFIG_listenPort:-8080}"

echo "Starting Cassandra exporter"
echo "JVM_OPTS: $JVM_OPTS"
cp /etc/cassandra_exporter/config.yml /tmp/config.yml
while IFS='=' read -r name value ; do
  if [[ $name == 'CASSANDRA_EXPORTER_CONFIG_'* ]]; then
    val="${!name}"
    echo "$name $val"
    field=$(echo $name | sed -r 's/CASSANDRA_EXPORTER_CONFIG_(.+)/\1/')
    sed -ri "s/^($field):.*/\1: $val/" "/tmp/config.yml"
  fi
done < <(env)

host=$(grep -m1 'host:' /tmp/config.yml | cut -d ':' -f2)
port=$(grep -m1 'host:' /tmp/config.yml | cut -d ':' -f3)

while ! nc -z $host $port; do
  echo "Waiting for Cassandra JMX to start on $host:$port"
  sleep 1
done

/sbin/dumb-init java ${JVM_OPTS} -jar /opt/cassandra_exporter/cassandra_exporter.jar /tmp/config.yml
