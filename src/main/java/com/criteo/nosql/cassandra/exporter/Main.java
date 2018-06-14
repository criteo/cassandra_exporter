package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.*;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

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

        if(cfg.getHost().contains(",")) {
            String[] hosts = cfg.getHost().split(",");
            ExecutorService pool = Executors.newFixedThreadPool(hosts.length);
            for(String host: hosts) {
                pool.submit(() -> runForNode(cfg, host));
            }

            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }

        JmxScraper scrapper = new JmxScraper(String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", cfg.getHost()), cfg.getUser(), cfg.getPassword(), cfg.getSSL(), cfg.getBlacklist(), cfg.getMaxScrapFrequencyInSec(), "");
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

    private static void runForNode(final Config cfg, final String host){

        logger.info("Starting scraping for {}", host);
        JmxScraper scrapper = new JmxScraper(String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", host), cfg.getUser(), cfg.getPassword(), cfg.getSSL(), cfg.getBlacklist(), cfg.getMaxScrapFrequencyInSec(), host);
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

}
