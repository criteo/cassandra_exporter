# Cassandra Exporter

<p align="center">
  <img src="https://github.com/criteo/cassandra_exporter/raw/master/logo.png" alt="logo"/>
</p>

### Description

Cassandra exporter is a standalone application made to export Cassandra metrics throught a prometheus endpoint.
This project is at the base a fork of [JMX exporter](https://github.com/prometheus/jmx_exporter) but aims to an easier integration with Cassandra.

Specifically, this project brings :
 - [x] Exporting EstimatedHistogram metrics specific to Cassandra
 - [x] Filtering on mbean's attributes
 - [x] Metrics naming that respect the mbean hierarchie
 - [x] Comprehensive config file

One contreversial choice the project makes, is to not let prometheus drives the scrapping frequency. This decision has been took because a lot of Cassandra metrics are expensive to scrap and can hinder the performance of the node.
As we don't want this kind of situation to happen in production the scrap frequency is restricted via the configuration of Cassandra Exporter.


### How to use

To start the application `java -jar cassandra_exporter.jar config.yml`

You can have a look at a full configuration file [here](https://github.com/criteo/cassandra_exporter/blob/master/config.yml)
The 2 main parts are :
 1. blacklist
 1. maxScrapFrequencyInSec
 
In the `blacklist` block you specify the metrics you don't want the exporter to scrap. This important as JMX is an RPC mechanism and you don't want to trigger some of those RPC. For example mbeans endpoint from `org:apache:cassandra:db:.*` does not expose any metrics but are used to trigger actions on Cassandra's nodes.

In the `maxScrapFrequencyInSec` you specify the metrics you want to be scrapped at which frequency.
Basically, from the set of all mbeans, first the blacklist is applied to filter this set and in 2nd the `maxScrapFrequencyInSec` is applied as a whitelist to filter the resulting set.

As an example, if we take as input set the metrics `{a, b, c}` and the config file is 
```yaml
blacklist:
  - a
maxScrapFrequencyInSec:
  50:
    - .*
  3600:
    - b
```
Cassandra Exporter will have the following behavior.
1. The metrics matching the blacklisted entries will never be scrapped, here the metric `a` won't be available
1. In reverse order of frequency the metrics matching `maxScrapFrequencyInSec` will be scrapped
   1. Metric `b` will be scrapped every hour
   1. Remaining metrics will be scrapped every 50s, here only `c`


### How to debug

Run the program with the following options `java -Dorg.slf4j.simpleLogger.defaultLogLevel=trace -jar cassandra_exporter.jar config.yml --oneshot`.
You will get the duration of how long it took to scrap individual MBean, this is useful to understand which metrics are expansive to scrap.

Goods source of information in order to understand what Mbeans are doing is :
 1. https://cassandra.apache.org/doc/latest/operating/metrics.html
 1. https://github.com/apache/cassandra/tree/trunk/src/java/org/apache/cassandra/metrics
 1. http://thelastpickle.com/blog/2017/12/05/datadog-tlp-dashboards.html
 1. https://www.youtube.com/watch?v=Q9AAR4UQzMk
