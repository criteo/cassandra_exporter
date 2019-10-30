package com.criteo.nosql.cassandra.exporter;

import org.junit.Test;

import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class ConfigTest {
    Logger logger = LoggerFactory.getLogger(ConfigTest.class);

    @Test
    public void test_additional_envvars_parsed() {

        Optional<Config> configWithAdditionalEnvvars = Config.fromFile("src/test/resources/config_tests/config_with_additional_envvars_regexp.yml");
        assertTrue(configWithAdditionalEnvvars.isPresent());
        assertTrue(configWithAdditionalEnvvars.get().getAdditionalLabelsFromEnvvars().isPresent());

        assertEquals(Pattern.compile("^ADDL\\_(.*)$").pattern(), configWithAdditionalEnvvars.get().getAdditionalLabelsFromEnvvars().get().pattern());

    }

}
