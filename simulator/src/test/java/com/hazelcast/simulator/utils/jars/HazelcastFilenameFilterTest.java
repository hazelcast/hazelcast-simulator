package com.hazelcast.simulator.utils.jars;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HazelcastFilenameFilterTest {

    private final HazelcastFilenameFilter filter = new HazelcastFilenameFilter();
    private final File dir = null;

    @Test
    public void accept_mustEndsWithJAR() {
        assertDeniesName("hazelcast-client-3.5-SNAPSHOT-jar");
    }

    @Test
    public void accept_mustStartWithHazelcast() {
        assertDeniesName("client-3.5-SNAPSHOT.jar");
    }

    @Test
    public void accept_doesntAcceptSources() {
        assertDeniesName("hazelcast-3.5-SNAPSHOT-sources.jar");
    }

    @Test
    public void accept_doesntAcceptTests() {
        assertDeniesName("hazelcast-3.5-SNAPSHOT-tests.jar");
    }

    @Test
    public void accept_doesntAcceptOriginal() {
        assertDeniesName("original-hazelcast-3.5-SNAPSHOT.jar");
    }

    @Test
    public void accept_doesntAcceptCodeGenerator() {
        assertDeniesName("hazelcast-code-generator-3.5-SNAPSHOT.jar");
    }

    @Test
    public void accept_member() {
        assertAcceptsName("hazelcast-3.5-SNAPSHOT.jar");
    }

    @Test
    public void accept_client() {
        assertAcceptsName("hazelcast-client-3.5-SNAPSHOT.jar");
    }

    private void assertAcceptsName(String filename) {
        assertTrue(filter.accept(dir, filename));
    }

    private void assertDeniesName(String filename) {
        assertFalse(filter.accept(dir, filename));
    }
}
