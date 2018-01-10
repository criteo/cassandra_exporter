package com.criteo.nosql.cassandra.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

public final class Config {

    public static final String DEFAULT_PATH = "config.yml";
    public static Optional<Config> fromFile(String filePath) {
        Logger logger = LoggerFactory.getLogger(Config.class);
        logger.info("Loading yaml config from {}", filePath);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            Config cfg = mapper.readValue(new File(filePath), Config.class);
            logger.trace(cfg.toString());
            return Optional.of(cfg);
        } catch (Exception e) {
            logger.error("Cannot load config file", e);
            return Optional.empty();
        }
    }

    private String host;
    private List<String> blacklist;
    private boolean ssl;
    private int listenPort;
    private String user;
    private String password;
    private SortedMap<Integer, List<String>> maxScrapFrequencyInSec;

    public SortedMap<Integer, List<String>> getMaxScrapFrequencyInSec() {
        return maxScrapFrequencyInSec;
    }

    public Optional<String> getUser() {
        return user == null ? Optional.empty() : Optional.of(user);
    }

    public Optional<String> getPassword() {
        return password == null ? Optional.empty() : Optional.of(password);
    }


    public int getListenPort() {
        return listenPort;
    }

    public String getHost() {
        return host;
    }

    public List<String> getBlacklist() {
        return blacklist;
    }

    public boolean getSSL() {
        return ssl;
    }
}

