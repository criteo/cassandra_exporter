package com.criteo.nosql.cassandra.exporter;

import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

public class Main {


    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Main.class);

        String configPath = args.length > 0 ? args[0] : Config.DEFAULT_PATH;
        Optional<Config> cfgO = Config.fromFile(configPath);

        if (!cfgO.isPresent()) {
            logger.error("Cannot parse config file present at {}", configPath);
            return;
        }

        Config cfg = cfgO.get();
        HTTPServer server = new HTTPServer(cfg.getListenPort());


        JmxScraper scrapper = new JmxScraper(String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", cfg.getHost()), cfg.getUser(), cfg.getPassword(), cfg.getSSL(), cfg.getBlacklist(), cfg.getMaxScrapFrequencyInSec());

        boolean isOneShot = Arrays.asList(args).contains("--oneshot");
        if (isOneShot) {
            scrapper.run(false);
            System.exit(0);
        } else {
            try {
                scrapper.run(true);
            } catch (Exception e) {
                logger.error("Scrapper stopped due to uncaught exception", e);
            }
        }



    }

}
