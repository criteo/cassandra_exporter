package com.criteo.nosql.cassandra.exporter;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class MainTest {

    private static class AdditionalEnvvars {

        private final static Map<String, String> envWithMatchingVars = new HashMap<String, String>() {{
            put("foo", "bar");
            put("ADDL_relevant-envvar-1", "relevant-value-1");
            put("ADDL_relevant-envvar-2", "relevant-value-2");
        }};

        private final static Map<String, String> envWithNoMatchingVars = new HashMap<String, String>() {{
            put("foo", "bar");
            put("IRRELEVANT-ENVVAR-1", "irrelevant-value-1");
            put("IRRELEVANT-ENVVAR-2", "irrelevant-value-2");
        }};

        private final static Pattern findRelevantEnvvarsGroup = Pattern.compile("^ADDL\\_(.*)$");
        private final static Pattern findRelevantEnvvarsNoGroup = Pattern.compile("^ADDL\\_.*$");
    }


    @Test
    public void test_findAdditionalLabelsInEnvironment_with_group_pattern_present_and_matching_envvars() {

        assertEquals(new HashMap<String, String>() {{
            put("relevant-envvar-1", "relevant-value-1");
            put("relevant-envvar-2", "relevant-value-2");
        }}, Main.findAdditionalLabelsInEnvironment(AdditionalEnvvars.envWithMatchingVars, Optional.of(AdditionalEnvvars.findRelevantEnvvarsGroup)));

    }

    @Test
    public void test_findAdditionalLabelsInEnvironment_with_pattern_present_and_no_matching_envvars() {

        assertEquals(new HashMap<String, String>() {{
            put("ADDL_relevant-envvar-1", "relevant-value-1");
            put("ADDL_relevant-envvar-2", "relevant-value-2");
        }}, Main.findAdditionalLabelsInEnvironment(AdditionalEnvvars.envWithMatchingVars, Optional.of(AdditionalEnvvars.findRelevantEnvvarsNoGroup)));
    }

    @Test
    public void test_findAdditionalLabelsInEnvironment_with_no_pattern_present() {

        assertEquals(Collections.emptyMap(), Main.findAdditionalLabelsInEnvironment(AdditionalEnvvars.envWithMatchingVars, Optional.empty()));
    }

}
