package com.criteo.nosql.cassandra.exporter;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class JmxScraperTest {

    @Test
    public void test_concat_concatenates_array_contents() {
        String[] arrayA = new String[]{"foo","bar","1"};
        String[] arrayB = new String[]{"baz","2"};

        assertArrayEquals(new String[]{"foo","bar","1", "baz", "2"}, JmxScraper.concat(arrayA, arrayB));
    }

    @Test
    public void test_concat_results_in_generic_array_of_expected_type_parameter() {
        String[] arrayA = new String[]{"foo","bar","1"};
        String[] arrayB = new String[]{"baz","2"};

        assertThat(JmxScraper.concat(arrayA, arrayB), instanceOf(String[].class));
    }

}
