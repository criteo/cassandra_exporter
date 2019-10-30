package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {


        String configPath = args.length > 0 ? args[0] : Config.DEFAULT_PATH;
        Optional<Config> cfgO = Config.fromFile(configPath);

        if (!cfgO.isPresent()) {
            logger.error("Cannot parse config file present at {}", configPath);
            return;
        }

        Config cfg = cfgO.get();
        boolean isOneShot = Arrays.asList(args).contains("--oneshot");
        HTTPServer server = new HTTPServer(cfg.getListenAddress(), cfg.getListenPort());
        JmxScraper scrapper = new JmxScraper(String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", cfg.getHost()), cfg.getUser(), cfg.getPassword(), cfg.getSSL(), cfg.getBlacklist(), cfg.getMaxScrapeFrequencyInSec(), findAdditionalLabelsInEnvironment(System.getenv(), cfg.getAdditionalLabelsFromEnvvars()));

        if (isOneShot) {
            scrapper.run(false);
            System.exit(0);
        }

        for (; ; ) {
            try {
                scrapper.run(true);
            } catch (Exception e) {
                logger.error("Scrapper stopped due to uncaught exception", e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.exit(0);
            }
        }
    }

    public static Map<String, String> findAdditionalLabelsInEnvironment(Map<String, String> environment, Optional<Pattern> matchNames) {
        if (matchNames.isPresent()) {


            return environment.entrySet().stream()
                    .filter(e -> matchNames.get().matcher(e.getKey()).matches())
                    .collect(Collectors.toMap(e -> {
                        Matcher m = matchNames.get().matcher(e.getKey());
                        m.matches(); // guaranteed to pass due to .filter above
                        if (m.groupCount() > 0) {
                            return m.group(1);
                        }
                        return e.getKey();
                    }, e -> e.getValue()));

        }

        return Collections.emptyMap();
    }

}
