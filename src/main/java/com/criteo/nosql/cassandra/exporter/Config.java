package com.criteo.nosql.cassandra.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.regex.Pattern;

public final class Config {

    public static final String DEFAULT_PATH = "config.yml";
    private String host;
    private List<String> blacklist;
    private boolean ssl;
    private int listenPort;
    private String listenAddress = "0.0.0.0";
    private String user;
    private String password;
    private SortedMap<Integer, List<String>> maxScrapFrequencyInSec;
    private Pattern additionalLabelsFromEnvvars;

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

    public SortedMap<Integer, List<String>> getMaxScrapFrequencyInSec() {
        return maxScrapFrequencyInSec;
    }

    public Optional<String> getUser() {
        return user == null ? Optional.empty() : Optional.of(user);
    }

    public Optional<String> getPassword() {
        return password == null ? Optional.empty() : Optional.of(password);
    }

    public String getListenAddress() {
        return listenAddress;
    }

    public int getListenPort() {
        return listenPort;
    }

    public String getHost() {
        return host;
    }

    public List<String> getBlacklist() {
        return blacklist == null ? Collections.emptyList() : blacklist;
    }

    public boolean getSSL() {
        return ssl;
    }

    public Optional<Pattern> getAdditionalLabelsFromEnvvars() { return additionalLabelsFromEnvvars == null ? Optional.empty() : Optional.of(additionalLabelsFromEnvvars); }
}

