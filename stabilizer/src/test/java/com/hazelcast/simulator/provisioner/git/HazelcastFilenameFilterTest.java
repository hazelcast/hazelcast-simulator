package com.hazelcast.simulator.provisioner.git;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class HazelcastFilenameFilterTest {
    private HazelcastFilenameFilter filter;
    private File dir;

    @Before
    public void setUp() {
        filter = new HazelcastFilenameFilter();
        dir = null;
    }

    @Test
    public void accept_mustEndsWithJAR() {
        assertDeniesName("hazelcast-client-3.5-SNAPSHOT");
    }

    @Test
    public void accept_mustContainsHazelcast() {
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
