package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(VersionUtils.class);
    }

    @Test
    public void testMinVersion() {
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5-EA"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5-RC"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5-RC1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1-EA"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1-RC"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.4", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5-EA"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5-RC"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5-RC1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1-EA"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1-RC"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.4.2", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(VersionUtils.isMinVersion("3.5", "3.5"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5-EA"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5-RC"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5-RC1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1-SNAPSHOT"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1-EA"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1-RC"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1-RC1"));
        assertTrue(VersionUtils.isMinVersion("3.5", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(VersionUtils.isMinVersion("3.5-SNAPSHOT", "3.5"));

        assertFalse(VersionUtils.isMinVersion("3.6", "3.5"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5-SNAPSHOT"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5-EA"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5-RC"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5-RC1"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5-RC1-SNAPSHOT"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1-SNAPSHOT"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1-EA"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1-RC"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1-RC1"));
        assertFalse(VersionUtils.isMinVersion("3.6", "3.5.1-RC1-SNAPSHOT"));
    }
}
