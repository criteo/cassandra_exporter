package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.naming.Context;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;


public class JmxScraper {
    private final Gauge stats;
    private static final Logger logger = LoggerFactory.getLogger(JmxScraper.class);
    private static final double[] offsetPercentiles = new double[]{0.5, 0.75, 0.95, 0.98, 0.99};
    private static final String metricSeparator = ":";
    private static final Map<String, MBeanAttributeInfo[]> mBeansAttributesCache = new HashMap<>();

    private final String jmxUrl;
    private final Pattern PATTERN = Pattern.compile("(:type=|,[^=]+=|\\.)");
    private final List<Pattern> blacklist;
    private final TreeMap<Integer, List<Pattern>> scrapFrequencies;
    private final Map<Integer, Long> lastScrapes;
    private final Map<String, Object> jmxEnv;
    private final String[] additionalLabelValues;


    public JmxScraper(String jmxUrl, Optional<String> username, Optional<String> password, boolean ssl, List<String> blacklist, SortedMap<Integer, List<String>> scrapFrequencies, Map<String, String> additionalLabels) {
        this.jmxUrl = jmxUrl;
        this.blacklist = blacklist.stream().map(Pattern::compile).collect(toList());
        this.scrapFrequencies = new TreeMap<>();
        this.lastScrapes = new HashMap<>(scrapFrequencies.size());
        String[] additionalLabelKeys = additionalLabels.keySet().stream().toArray(String[]::new);
        this.additionalLabelValues = additionalLabels.values().stream().toArray(String[]::new);

        this.stats = Gauge.build()
                .name("cassandra_stats")
                .help("node stats")
                .labelNames(concat(new String[]{"cluster", "datacenter", "keyspace", "table", "name"}, additionalLabelKeys))
                .register();

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

    private void updateStats(NodeInfo nodeInfo, String metricName, Double value) {

        if (metricName.startsWith("org:apache:cassandra:metrics:keyspace:")) {
            int pathLength = "org:apache:cassandra:metrics:keyspace:".length();
            int pos = metricName.indexOf(':', pathLength);
            String keyspaceName = metricName.substring(pathLength, pos);

            this.stats.labels(concat(new String[] {nodeInfo.clusterName, nodeInfo.datacenterName,
                    nodeInfo.keyspaces.contains(keyspaceName) ? keyspaceName : "", "", metricName}, this.additionalLabelValues)).set(value);
            return;
        }

        // Cassandra 3.x path style to get table info
        if (metricName.startsWith("org:apache:cassandra:metrics:table:")) {
            int pathLength = "org:apache:cassandra:metrics:table:".length();
            int keyspacePos = metricName.indexOf(':', pathLength);
            int tablePos = metricName.indexOf(':', keyspacePos + 1);
            String keyspaceName = metricName.substring(pathLength, keyspacePos);
            String tableName = tablePos > 0 ? metricName.substring(keyspacePos + 1, tablePos) : "";

            if (nodeInfo.keyspaces.contains(keyspaceName) && nodeInfo.tables.contains(tableName)) {
                this.stats.labels(concat(new String[] {nodeInfo.clusterName, nodeInfo.datacenterName, keyspaceName, tableName, metricName}, additionalLabelValues)).set(value);
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

            if (nodeInfo.keyspaces.contains(keyspaceName) && nodeInfo.tables.contains(tableName)) {
                this.stats.labels(concat(new String[] {nodeInfo.clusterName, nodeInfo.datacenterName, keyspaceName, tableName, metricName}, additionalLabelValues)).set(value);
                return;
            }
        }


        if (metricName.startsWith("org:apache:cassandra:metrics:compaction:pendingtasksbytablename:")) {
            int pathLength = "org:apache:cassandra:metrics:compaction:pendingtasksbytablename:".length();
            int keyspacePos = metricName.indexOf(':', pathLength);
            int tablePos = metricName.indexOf(':', keyspacePos + 1);
            String keyspaceName = metricName.substring(pathLength, keyspacePos);
            String tableName = tablePos > 0 ? metricName.substring(keyspacePos + 1, tablePos) : "";

            if (nodeInfo.keyspaces.contains(keyspaceName) && nodeInfo.tables.contains(tableName)) {
                this.stats.labels(concat(new String[]{nodeInfo.clusterName, nodeInfo.datacenterName, keyspaceName, tableName, metricName}, additionalLabelValues)).set(value);
                return;
            }
        }


        this.stats.labels(concat(new String[]{nodeInfo.clusterName, nodeInfo.datacenterName, "", "", metricName}, additionalLabelValues)).set(value);
    }

    private Boolean shouldRemove(NodeInfo nodeInfo, Collector.MetricFamilySamples.Sample sample) {
        String keyspace = sample.labelValues.get(2);
        String table = sample.labelValues.get(3);
        return (!"".equals(keyspace) && !nodeInfo.keyspaces.contains(keyspace)) || (!"".equals(table) && !nodeInfo.tables.contains(table));
    }


    /**
     * Remove metrics for drop keyspaces/tables
     */
    private void removeMetrics(NodeInfo nodeInfo) {
        this.stats.collect()
                .stream()
                .flatMap(metrics -> metrics.samples.stream())
                .filter(sample -> shouldRemove(nodeInfo, sample))
                .forEach(sample -> this.stats.remove(sample.labelValues.toArray(new String[0])));
    }

    public void run(final boolean forever) throws Exception {

        this.stats.clear();
        try (JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), jmxEnv)) {
            final MBeanServerConnection beanConn = jmxc.getMBeanServerConnection();

            do {
                final long now = System.currentTimeMillis();

                // If we can't get the node info, exit the run early in order to avoid creating stale metrics
                // that will never be cleaned after that
                // This situation can appear if the node start to be unresponsive and that some jmx operation timeouts
                final Optional<NodeInfo> nodeInfo = NodeInfo.getNodeInfo(beanConn);
                if (!nodeInfo.isPresent()) return;

                MBeanAttributeInfo attr = new MBeanAttributeInfo("", "", "", false, false, false);
                beanConn.queryMBeans(null, null).stream()
                        .map(objectInstance -> new MBeanInfo(getMetricPath(objectInstance.getObjectName(), attr), objectInstance.getObjectName(), attr))
                        .filter(mbean -> blacklist.stream().noneMatch(pattern -> pattern.matcher(mbean.metricName).matches()))
                        .flatMap(mBeanInfo -> toMBeanInfos(beanConn, mBeanInfo))
                        .filter(m -> shouldScrap(m, now))
                        .forEach(mBean -> updateMetric(beanConn, mBean, nodeInfo.get()));

                removeMetrics(nodeInfo.get());

                lastScrapes.forEach((k, lastScrape) -> {
                    if (now - lastScrape >= k) lastScrapes.put(k, now);
                });

                final long duration = System.currentTimeMillis() - now;
                logger.info("Scrap took {}ms for the whole run", duration);

                // don't go lower than 10 sec
                if (forever) Thread.sleep(Math.max(scrapFrequencies.firstKey() - duration, 10 * 1000));
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
        if (blacklist.stream().anyMatch(pattern -> pattern.matcher(mBeanInfo.metricName).matches())) {
            return false;
        }

        for (Map.Entry<Integer, List<Pattern>> e : scrapFrequencies.descendingMap().entrySet()) {
            for (Pattern p : e.getValue()) {
                if (p.matcher(mBeanInfo.metricName).matches()) {
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
     * @param beanConn The JMX connexion
     */
    private Stream<MBeanInfo> toMBeanInfos(MBeanServerConnection beanConn, MBeanInfo mBeanInfo) {
        try {
            ObjectName mbeanName = mBeanInfo.mBeanName;
            String name = "" + mbeanName.getKeyPropertyList().size() + mbeanName.getKeyProperty("type") + mbeanName.getKeyProperty("name");
            MBeanAttributeInfo[] info = mBeansAttributesCache.computeIfAbsent(name, xx -> {
                try {
                    return beanConn.getMBeanInfo(mbeanName).getAttributes();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            return Arrays.stream(info)
                    .filter(MBeanAttributeInfo::isReadable)
                    //TODO: Refactor get MetricPath
                    .map(mBeanAttributeInfo -> new MBeanInfo(mBeanInfo.metricName + mBeanAttributeInfo.getName().toLowerCase(), mbeanName, mBeanAttributeInfo));
        } catch (Exception e) {
            logger.error(" Error when scraping mbean {}", mBeanInfo.mBeanName, e);
            return Stream.empty();
        }
    }

    /**
     * Return the formatted metric for a given MBean and attribute
     *
     * @param mbeanName Mbean object
     * @param attr      Specific attribute of this Mbean
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
        } catch (Exception e) {
            if (e instanceof RuntimeMBeanException && e.getCause() != null && e.getCause() instanceof UnsupportedOperationException) {
                return;
            }
            logger.error("Cannot get value for {} {}", mBeanInfo.metricName, mBeanInfo.attribute.getName(), e);
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

            case "java.util.List":
                updateStats(nodeInfo, mBeanInfo.metricName, Double.valueOf(((List) value).size()));
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
                    
                } else if (first == '{' && mBeanInfo.metricName.equalsIgnoreCase("org:apache:cassandra:metrics:compaction:pendingtasksbytablename:value")) {
                    HashMap<String, HashMap<String, Integer>> pendingTasks = (HashMap<String, HashMap<String, Integer>>) value;
                    for (String keyspace : pendingTasks.keySet()) {
                        for (String table : pendingTasks.get(keyspace).keySet()) {
                            String labels = String.join(":", keyspace, table, "value");
                            String metricName = mBeanInfo.metricName.replace("value", labels);
                            updateStats(nodeInfo, metricName, pendingTasks.get(keyspace).get(table).doubleValue());
                        }
                    }

                    // https://books.google.fr/books?id=BvsVuph6ehMC&pg=PA82
                    // EstimatedHistogram are object for JMX but are long[] behind
                } else if (str.startsWith(long[].class.getName())) {

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

    public static <T> T[] concat(T[] a, T[] b) {
        T[] finalArray = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, finalArray, a.length, b.length);
        return finalArray;
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

        private NodeInfo(String clusterName, String datacenterName, Set<String> keyspaces, Set<String> tables) {
            this.clusterName = clusterName;
            this.datacenterName = datacenterName;
            this.keyspaces = keyspaces;
            this.tables = tables;
        }

        static Optional<NodeInfo> getNodeInfo(MBeanServerConnection beanConn) {
            String clusterName;
            String datacenterName;
            Set<String> keyspaces = new HashSet<>();
            Set<String> tables = new HashSet<>();

            try {
                clusterName = beanConn.getAttribute(ObjectName.getInstance("org.apache.cassandra.db:type=StorageService"), "ClusterName").toString();
            } catch (Exception e) {
                logger.error("Cannot retrieve the cluster name information for the node", e);
                return Optional.empty();
            }

            try {
                String hostID = beanConn.getAttribute(ObjectName.getInstance("org.apache.cassandra.db:type=StorageService"), "LocalHostId").toString();
                Map<String, String> hostIdToEndpoint = (Map<String, String>) beanConn.getAttribute(ObjectName.getInstance("org.apache.cassandra.db:type=StorageService"), "HostIdToEndpoint");
                Object opParams[] = {hostIdToEndpoint.get(hostID)};
                String opSig[] = {String.class.getName()};
                datacenterName = beanConn.invoke(ObjectName.getInstance("org.apache.cassandra.db:type=EndpointSnitchInfo"), "getDatacenter", opParams, opSig).toString();
            } catch (Exception e) {
                logger.error("Cannot retrieve the datacenter name information for the node", e);
                return Optional.empty();
            }

            try {
                Set<ObjectName> names = beanConn.queryNames(ObjectName.getInstance("org.apache.cassandra.db:type=ColumnFamilies,keyspace=*,columnfamily=*"), null);
                for (ObjectName name : names) {
                    String[] values = name.toString().split("[=,]");
                    keyspaces.add(values[3].toLowerCase());
                    tables.add(values[5].toLowerCase());
                }
            } catch (Exception e) {
                logger.error("Cannot retrieve keyspaces/tables information", e);
                return Optional.empty();
            }

            return Optional.of(new NodeInfo(clusterName, datacenterName, keyspaces, tables));
        }
    }
}

