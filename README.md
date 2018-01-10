# Cassandra Exporter

### Description

Cassandra exporter is a standalone application made to export Cassandra metrics throught a prometheus endpoint.
This project is at the base a fork of [JMX exporter](https://github.com/prometheus/jmx_exporter) but aims to an easier integration with Cassandra.

Specifically, this project brings :
 - [x] Exporting EstimatedHistogram metrics specific to Cassandra
 - [x] Filtering on mbean's attributes
 - [x] Metrics naming that respect the mbean hierarchie
 - [x] Comprehensive config file

One contreversial choice the project makes, is to not let prometheus drives the frequency of the scrapping. This decision has been took because a lot of Cassandra metrics are expensive to scrap and can inder the performance of the node.
As we don't want this kind of situation to happen in production the scrap frequency is restricted via the configuration of Cassandra Exporter.


### How to use
