package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.naming.Context;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;


public class JmxScraper {
    private static final Logger logger = LoggerFactory.getLogger(JmxScraper.class);

    static final Gauge STATS = Gauge.build()
            .name("cassandra_stats")
            .help("node stats")
            .labelNames("name")
            .register();

    private final String jmxUrl;
    private final Optional<String> username;
    private final Optional<String> password;
    private final boolean ssl;
    private final Pattern PATTERN = Pattern.compile("(:type=|,[^=]+=|\\.)");
    private final List<Pattern> blacklist;
    private final TreeMap<Integer, List<Pattern>> scrapFrequencies;
    private final Map<Integer, Long> lastScrapes;
    private final List<MBeanInfo> mBeanInfos;
    private final Map<String, Object> jmxEnv;


    public JmxScraper(String jmxUrl, Optional<String> username, Optional<String> password, boolean ssl, List<String> blacklist, SortedMap<Integer, List<String>> scrapFrequencies) {
        this.jmxUrl = jmxUrl;
        this.username = username;
        this.password = password;
        this.ssl = ssl;
        this.blacklist = blacklist.stream().map(Pattern::compile).collect(toList());
        this.scrapFrequencies = new TreeMap<>();
        this.lastScrapes = new HashMap<>(scrapFrequencies.size());
        this.mBeanInfos = new ArrayList<>();

        scrapFrequencies.forEach((k, v) -> {
            this.scrapFrequencies.put(k * 1000, v.stream().map(Pattern::compile).collect(toList()));
            this.lastScrapes.put(k * 1000, 0L);
        });

        jmxEnv = new HashMap<>();
        username.ifPresent(user -> {
            String[] credent = new String[] {user, password.orElse("")};
            jmxEnv.put(javax.management.remote.JMXConnector.CREDENTIALS, credent);
        });

        if (ssl) {
            jmxEnv.put(Context.SECURITY_PROTOCOL, "ssl");
            SslRMIClientSocketFactory clientSocketFactory = new SslRMIClientSocketFactory();
            jmxEnv.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientSocketFactory);
            jmxEnv.put("com.sun.jndi.rmi.factory.socket", clientSocketFactory);
        }
    }

    /**
      * Get a list of mbeans on host_port and scrape their values.
      *
      * Values are passed to the receiver in a single thread.
      */
    public void run(final boolean forever) throws Exception {

        try(JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), jmxEnv)) {
            final MBeanServerConnection beanConn = jmxc.getMBeanServerConnection();

            do {
                final long now = System.currentTimeMillis();

                for(ObjectInstance name: beanConn.queryMBeans(null, null)) {
                    collectMBeanInfos(beanConn, name.getObjectName());
                }

                mBeanInfos.stream()
                        .filter(m -> shouldScrap(m, now))
                        .forEach(mBean -> updateMetric(beanConn, mBean));


                lastScrapes.forEach((k,lastScrape) -> {
                   if(now - lastScrape > k) lastScrapes.put(k, now);
                });

                final long duration = System.currentTimeMillis() - now;
               logger.info("Scrap took {}ms for the whole run", duration);
               this.mBeanInfos.clear();

               // don't go lower than 10 sec
               if(forever) Thread.sleep(Math.max(scrapFrequencies.firstKey() - duration, 10 * 1000));
            } while (forever);
        }
    }

    private boolean shouldScrap(final MBeanInfo mBeanInfo, final long now) {
        if(blacklist.stream().anyMatch(pattern -> pattern.matcher(mBeanInfo.metricName).matches())) {
            return false;
        }

        for(Map.Entry<Integer, List<Pattern>> e: scrapFrequencies.descendingMap().entrySet()) {
            for(Pattern p: e.getValue()) {
                if(p.matcher(mBeanInfo.metricName).matches()) {
                    return now - lastScrapes.get(e.getKey()) > e.getKey();
                }
            }
        }
        return false;
    }

    private void collectMBeanInfos(MBeanServerConnection beanConn, ObjectName mbeanName) {
        try {
            for(MBeanAttributeInfo attr: beanConn.getMBeanInfo(mbeanName).getAttributes()) {
                if (!attr.isReadable()) {
                    logger.debug("Cannot read attribute {} of {}", attr.getName(), mbeanName);
                    continue;
                }
                final String name = getMetricPath(mbeanName, attr);
                this.mBeanInfos.add(new MBeanInfo(name,  mbeanName, attr));
            }
        } catch (Exception e) {
            logger.error(" Error when scraping mbean {}", mbeanName, e);
        }
    }

    private String getMetricPath(ObjectName mbeanName, MBeanAttributeInfo attr) {
        String properties = PATTERN.matcher(mbeanName.toString()).replaceAll(":").replace(' ', '_')
                            + ':' + attr.getName();
        return properties.toLowerCase();
    }

    /**
     * Recursive function for exporting the values of an mBean.
     * JMX is a very open technology, without any prescribed way of declaring mBeans
     * so this function tries to do a best-effort pass of getting the values/names
     * out in a way it can be processed elsewhere easily.
     */
    private void updateMetric(MBeanServerConnection beanConn, MBeanInfo mBeanInfo) {

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

        switch (mBeanInfo.attribute.getType()) {
            case "long":
            case "int":
            case "double":
                STATS.labels(mBeanInfo.metricName).set(((Number) value).doubleValue());
                break;

            case "boolean":
                STATS.labels(mBeanInfo.metricName).set(((Boolean) value) ? 1 : 0);
                break;

            case "java.lang.Object":
                String str = value.toString();
                if (str.charAt(0) > '0' && str.charAt(0) < '9') {
                    STATS.labels(mBeanInfo.metricName).set(Double.valueOf(value.toString()));
                }
                break;

            default:
                logger.debug("Cannot parse {} as it as an unknown type {} with value {}", mBeanInfo.mBeanName, mBeanInfo.attribute.getType(), value);
                break;
        }
        logger.trace("Scrapping took {}ms for {}", (System.currentTimeMillis() - start), mBeanInfo.metricName);
    }

    private static class MBeanInfo {
        public final String metricName;
        public final ObjectName mBeanName;
        public final MBeanAttributeInfo attribute;

        public MBeanInfo(String name, ObjectName mBeanName, MBeanAttributeInfo attribute) {
           this.metricName = name;
           this.attribute = attribute;
           this.mBeanName = mBeanName;
        }
    }
}

//        if (value instanceof CompositeData) {
//            logScrape(domain + beanProperties + attrName, "compositedata");
//            CompositeData composite = (CompositeData) value;
//            CompositeType type = composite.getCompositeType();
//            attrKeys = new LinkedList<>(attrKeys);
//            attrKeys.add(attrName);
//            for(String key : type.keySet()) {
//                String typ = type.getType(key).getTypeName();
//                Object valu = composite.get(key);
//                processBeanValue(
//                        domain,
//                        beanProperties,
//                        attrKeys,
//                        key,
//                        typ,
//                        type.getDescription(),
//                        valu);
//            }
//        } else if (value instanceof TabularData) {
//            // I don't pretend to have a good understanding of TabularData.
//            // The real world usage doesn't appear to match how they were
//            // meant to be used according to the docs. I've only seen them
//            // used as 'key' 'value' pairs even when 'value' is itself a
//            // CompositeData of multiple values.
//            logScrape(domain + beanProperties + attrName, "tabulardata");
//            TabularData tds = (TabularData) value;
//            TabularType tt = tds.getTabularType();
//
//            List<String> rowKeys = tt.getIndexNames();
//            LinkedHashMap<String, String> l2s = new LinkedHashMap<String, String>(beanProperties);
//
//            CompositeType type = tt.getRowType();
//            Set<String> valueKeys = new TreeSet<String>(type.keySet());
//            valueKeys.removeAll(rowKeys);
//
//            LinkedList<String> extendedAttrKeys = new LinkedList<String>(attrKeys);
//            extendedAttrKeys.add(attrName);
//            for (Object valu : tds.values()) {
//                if (valu instanceof CompositeData) {
//                    CompositeData composite = (CompositeData) valu;
//                    for (String idx : rowKeys) {
//                        l2s.put(idx, composite.get(idx).toString());
//                    }
//                    for(String valueIdx : valueKeys) {
//                        LinkedList<String> attrNames = extendedAttrKeys;
//                        String typ = type.getType(valueIdx).getTypeName();
//                        String name = valueIdx;
//                        if (valueIdx.toLowerCase().equals("value")) {
//                            // Skip appending 'value' to the name
//                            attrNames = attrKeys;
//                            name = attrName;
//                        }
//                        processBeanValue(
//                            domain,
//                            l2s,
//                            attrNames,
//                            name,
//                            typ,
//                            type.getDescription(),
//                            composite.get(valueIdx));
//                    }
//                } else {
//                    logScrape(domain, "not a correct tabulardata format");
//                }
//            }

