package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.naming.Context;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;


public class JmxScraper {
    static final Gauge STATS = Gauge.build()
            .name("cassandra_stats")
            .help("node stats")
            .labelNames("cluster", "datacenter", "node", "keyspace", "table", "name")
            .register();
    private static final Logger logger = LoggerFactory.getLogger(JmxScraper.class);
    private static final double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};
    private static final String metricSeparator = ":";

    private final String jmxUrl;
    private final Pattern PATTERN = Pattern.compile("(:type=|,[^=]+=|\\.)");
    private final List<Pattern> blacklist;
    private final TreeMap<Integer, List<Pattern>> scrapFrequencies;
    private final Map<Integer, Long> lastScrapes;
    private final Map<String, Object> jmxEnv;
    private final String nodeName;


    public JmxScraper(String jmxUrl, Optional<String> username, Optional<String> password, boolean ssl, List<String> blacklist, SortedMap<Integer, List<String>> scrapFrequencies, String node) {
        this.jmxUrl = jmxUrl;
        this.blacklist = blacklist.stream().map(Pattern::compile).collect(toList());
        this.scrapFrequencies = new TreeMap<>();
        this.lastScrapes = new HashMap<>(scrapFrequencies.size());
        this.nodeName = node;

        scrapFrequencies.forEach((k, v) -> {
            this.scrapFrequencies.put(k * 1000, v.stream().map(Pattern::compile).collect(toList()));
            this.lastScrapes.put(k * 1000, 0L);
        });

        jmxEnv = new HashMap<>();
        username.ifPresent(user -> {
            String[] credential = new String[]{user, password.orElse("")};
            jmxEnv.put(javax.management.remote.JMXConnector.CREDENTIALS, credential);
        });

        if (ssl) {
            jmxEnv.put(Context.SECURITY_PROTOCOL, "ssl");
            SslRMIClientSocketFactory clientSocketFactory = new SslRMIClientSocketFactory();
            jmxEnv.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientSocketFactory);
            jmxEnv.put("com.sun.jndi.rmi.factory.socket", clientSocketFactory);
        }
    }

    /**
     * Convert a Cassandra EstimatedHistogram value into known percentiles histogram
     *
     * @param counts the serialized value of the estimatedhistrogram
     * @return Percentiles of the histogram in the given order double[]{0.5, 0.75, 0.95, 0.98, 0.99};
     */
    private static double[] metricPercentilesAsArray(long[] counts) {
        // Copy-pasted mostly from https://github.com/apache/cassandra/blob/f59df2893b66b3a8715b9792679e51815982a542/src/java/org/apache/cassandra/tools/NodeProbe.java#L1223
        double[] result = new double[7];

        if (counts == null || counts.length == 0) {
            Arrays.fill(result, Double.NaN);
            return result;
        }

        long[] offsets = EstimatedHistogram.newOffsets(counts.length, false);
        EstimatedHistogram metric = new EstimatedHistogram(offsets, counts);

        if (metric.isOverflowed()) {
            logger.error("EstimatedHistogram overflowed larger than {}, unable to calculate percentiles", offsets[offsets.length - 1]);
            for (int i = 0; i < result.length; i++)
                result[i] = Double.NaN;
        } else {
            for (int i = 0; i < offsetPercentiles.length; i++)
                result[i] = metric.percentile(offsetPercentiles[i]);
        }
        result[5] = metric.min();
        result[6] = metric.max();
        return result;
    }

    private static void updateStats(NodeInfo nodeInfo, String metricName, Double value) {

       if(metricName.startsWith("org:apache:cassandra:metrics:keyspace:")) {
           int pathLength = "org:apache:cassandra:metrics:keyspace:".length();
           int pos = metricName.indexOf(':', pathLength);
           String keyspaceName = metricName.substring(pathLength, pos);

           STATS.labels(nodeInfo.clusterName, nodeInfo.datacenterName, nodeInfo.nodeName,
                   nodeInfo.keyspaces.contains(keyspaceName) ? keyspaceName : "", "", metricName).set(value);
           return;
       }

       // Cassandra 3.x path style to get table info
       if (metricName.startsWith("org:apache:cassandra:metrics:table:")) {
           int pathLength = "org:apache:cassandra:metrics:table:".length();
           int keyspacePos = metricName.indexOf(':', pathLength);
           int tablePos = metricName.indexOf(':', keyspacePos + 1);
           String keyspaceName = metricName.substring(pathLength, keyspacePos);
           String tableName = tablePos > 0 ? metricName.substring(keyspacePos + 1, tablePos) : "";

           if(nodeInfo.keyspaces.contains(keyspaceName) && nodeInfo.tables.contains(tableName)) {
               STATS.labels(nodeInfo.clusterName, nodeInfo.datacenterName, nodeInfo.nodeName, keyspaceName, tableName, metricName).set(value);
               return;
           }
       }

        // Cassandra 2.x path style to get table info
        if (metricName.startsWith("org:apache:cassandra:metrics:columnfamily:")) {
            int pathLength = "org:apache:cassandra:metrics:columnfamily:".length();
            int keyspacePos = metricName.indexOf(':', pathLength);
            int tablePos = metricName.indexOf(':', keyspacePos + 1);
            String keyspaceName = metricName.substring(pathLength, keyspacePos);
            String tableName = tablePos > 0 ? metricName.substring(keyspacePos + 1, tablePos) : "";

            if(nodeInfo.keyspaces.contains(keyspaceName) && nodeInfo.tables.contains(tableName)) {
                STATS.labels(nodeInfo.clusterName, nodeInfo.datacenterName, nodeInfo.nodeName, keyspaceName, tableName, metricName).set(value);
                return;
            }
        }

       STATS.labels(nodeInfo.clusterName, nodeInfo.datacenterName, nodeInfo.nodeName, "", "", metricName).set(value);
    }

    public void run(final boolean forever) throws Exception {

        STATS.clear();
        try(JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), jmxEnv)) {
            final MBeanServerConnection beanConn = jmxc.getMBeanServerConnection();

            do {
                final long now = System.currentTimeMillis();
                final NodeInfo nodeInfo = NodeInfo.getNodeInfo(beanConn, nodeName);
                beanConn.queryMBeans(null, null).stream()
                        .flatMap(objectInstance -> toMBeanInfos(beanConn, objectInstance.getObjectName()))
                        .filter(m -> shouldScrap(m, now))
                        .forEach(mBean -> updateMetric(beanConn, mBean, nodeInfo));


                lastScrapes.forEach((k,lastScrape) -> {
                    if (now - lastScrape >= k) lastScrapes.put(k, now);
                });

                final long duration = System.currentTimeMillis() - now;
               logger.info("Scrap took {}ms for the whole run", duration);

               // don't go lower than 10 sec
               if(forever) Thread.sleep(Math.max(scrapFrequencies.firstKey() - duration, 10 * 1000));
            } while (forever);
        }
    }

    /**
     * Return if we should scrap this MBean at a given point in time
     *
     * @param mBeanInfo Should we scrap this MBean
     * @param now       timestamp representing the point in time
     * @return True if we should scrap it, False if not
     */
    private boolean shouldScrap(final MBeanInfo mBeanInfo, final long now) {
        if(blacklist.stream().anyMatch(pattern -> pattern.matcher(mBeanInfo.metricName).matches())) {
            return false;
        }

        for(Map.Entry<Integer, List<Pattern>> e: scrapFrequencies.descendingMap().entrySet()) {
            for(Pattern p: e.getValue()) {
                if(p.matcher(mBeanInfo.metricName).matches()) {
                    return now - lastScrapes.get(e.getKey()) >= e.getKey();
                }
            }
        }
        return false;
    }

    /**
     * Return all possible MbeanInfo from a MBean path.
     * The main interest is to unroll of the attributes of an Mbeans in order to filter on it
     *
     * @param beanConn  The JMX connexion
     * @param mbeanName The MBean path
     */
    private Stream<MBeanInfo> toMBeanInfos(MBeanServerConnection beanConn, ObjectName mbeanName) {
        try {
            return Arrays.stream(beanConn.getMBeanInfo(mbeanName).getAttributes())
                    .filter(MBeanAttributeInfo::isReadable)
                    .map(mBeanAttributeInfo -> new MBeanInfo(getMetricPath(mbeanName, mBeanAttributeInfo), mbeanName, mBeanAttributeInfo));
        } catch (Exception e) {
            logger.error(" Error when scraping mbean {}", mbeanName, e);
            return Stream.empty();
        }
    }

    /**
     * Return the formatted metric for a given MBean and attribute
     *
     * @param mbeanName Mbean object
     * @param attr Specific attribute of this Mbean
     * @return the formatted metric name
     */
    private String getMetricPath(ObjectName mbeanName, MBeanAttributeInfo attr) {
        String properties = PATTERN.matcher(mbeanName.toString())
                .replaceAll(metricSeparator)
                .replace(' ', '_') + metricSeparator + attr.getName();
        return properties.toLowerCase();
    }

    /**
     * Update the metrics registry for a given MBeanInfo
     *
     * @param beanConn
     * @param mBeanInfo
     */
    private void updateMetric(MBeanServerConnection beanConn, MBeanInfo mBeanInfo, NodeInfo nodeInfo) {
        long start = System.currentTimeMillis();
        Object value = null;
        try {
            value = beanConn.getAttribute(mBeanInfo.mBeanName, mBeanInfo.attribute.getName());
        } catch(Exception e) {
            logger.error("Cannot get value for {}{}", mBeanInfo, mBeanInfo.attribute.getName(), e);
        }
        if (value == null) {
            return;
        }

        // Converting attribute to double
        switch (mBeanInfo.attribute.getType()) {
            case "long":
            case "int":
            case "double":
                updateStats(nodeInfo, mBeanInfo.metricName, ((Number) value).doubleValue());
                break;

            case "boolean":
                updateStats(nodeInfo, mBeanInfo.metricName, ((Boolean) value) ? 1.0 : 0.0);
                break;

            case "javax.management.openmbean.CompositeData":
                CompositeData data = ((CompositeData) value);
                CompositeType types = ((CompositeData) value).getCompositeType();
                for (String itemName : types.keySet()) {
                    switch (types.getType(itemName).getTypeName()) {
                        case "java.lang.Long":
                        case "java.lang.Double":
                        case "java.lang.Integer":
                            updateStats(nodeInfo, mBeanInfo.metricName + metricSeparator + itemName.toLowerCase(), ((Number) data.get(itemName)).doubleValue());
                            break;
                    }
                }
                break;

            case "java.lang.Object":
                String str = value.toString();
                Character first = str.charAt(0);

                //Most beans declared as Object are Double in disguise
                if (first >= '0' && first <= '9') {
                    updateStats(nodeInfo, mBeanInfo.metricName, Double.valueOf(str));

                } else if (first == '[') {

                    // This is ugly to redo the shouldScrap and metricName formating this late
                    // but EstimatedHistogram in cassandra are special case that we cannot detect before :(
                    double[] percentiles = metricPercentilesAsArray((long[]) value);
                    for (int i = 0; i < offsetPercentiles.length; i++) {
                        final String metricName = mBeanInfo.metricName.replace(":value", ":" + (int) (offsetPercentiles[i] * 100) + "thpercentile");
                        MBeanInfo info = new MBeanInfo(metricName, mBeanInfo.mBeanName, mBeanInfo.attribute);
                        if (shouldScrap(info, start)) {
                            updateStats(nodeInfo, metricName, percentiles[i]);
                        }
                    }

                    final String minMetricName = mBeanInfo.metricName.replace(":value", ":min");
                    MBeanInfo info = new MBeanInfo(minMetricName, mBeanInfo.mBeanName, mBeanInfo.attribute);
                    if (shouldScrap(info, start)) {
                        updateStats(nodeInfo, minMetricName, percentiles[5]);
                    }
                    final String metricName = mBeanInfo.metricName.replace(":value", ":max");
                    info = new MBeanInfo(metricName, mBeanInfo.mBeanName, mBeanInfo.attribute);
                    if (shouldScrap(info, start)) {
                        updateStats(nodeInfo, metricName, percentiles[6]);
                    }

                } else {
                    logger.debug("Cannot parse {} as it as an unknown type {} with value {}", mBeanInfo.mBeanName, mBeanInfo.attribute.getType(), value);
                }
                break;

            default:
                logger.debug("Cannot parse {} as it as an unknown type {} with value {}", mBeanInfo.mBeanName, mBeanInfo.attribute.getType(), value);
                break;
        }
        logger.trace("Scrapping took {}ms for {}", (System.currentTimeMillis() - start), mBeanInfo.metricName);
    }

    /**
     * POJO to hold information regarding a metric
     */
    private static class MBeanInfo {
        final String metricName;
        final ObjectName mBeanName;
        final MBeanAttributeInfo attribute;

        MBeanInfo(String name, ObjectName mBeanName, MBeanAttributeInfo attribute) {
           this.metricName = name;
           this.attribute = attribute;
           this.mBeanName = mBeanName;
        }
    }

    private static class NodeInfo {
        final String clusterName;
        final String datacenterName;
        final Set<String> keyspaces;
        final Set<String> tables;
        final String nodeName;

        private NodeInfo(String clusterName, String datacenterName, Set<String> keyspaces, Set<String> tables, String nodeName) {
            this.clusterName = clusterName;
            this.datacenterName = datacenterName;
            this.keyspaces = keyspaces;
            this.tables = tables;
            this.nodeName = nodeName;
        }

        static NodeInfo getNodeInfo(MBeanServerConnection beanConn, String nodeName) {
            String clusterName = "";
            String datacenterName = "";
            Set<String> keyspaces = new HashSet<>();
            Set<String> tables = new HashSet<>();

            try {
                 clusterName = beanConn.getAttribute(ObjectName.getInstance("org.apache.cassandra.db:type=StorageService"), "ClusterName").toString();
            } catch (Exception e) {
                logger.error("Cannot retrieve the cluster name information for the node", e);
            }

            try {
                datacenterName = beanConn.getAttribute(ObjectName.getInstance("org.apache.cassandra.db:type=EndpointSnitchInfo"), "Datacenter").toString();
            } catch (Exception e) {
                logger.error("Cannot retrieve the datacenter name information for the node", e);
            }

            try {
                Set<ObjectName> names = beanConn.queryNames(ObjectName.getInstance("org.apache.cassandra.db:type=ColumnFamilies,keyspace=*,columnfamily=*"), null);
                for(ObjectName name: names) {
                    String[] values = name.toString().split("[=,]");
                    keyspaces.add(values[3]);
                    tables.add(values[5]);
                }
            } catch (Exception e) {
                logger.error("Cannot retrieve keyspaces/tables information", e);
            }

            return new NodeInfo(clusterName, datacenterName, keyspaces, tables, nodeName);
        }
    }
}

