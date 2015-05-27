package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(VersionUtils.class);
    }

    @Test
    public void testMinVersion() {
        assertTrue(isMinVersion("3.4", "3.5"));
        assertTrue(isMinVersion("3.4", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5-EA"));
        assertTrue(isMinVersion("3.4", "3.5-RC"));
        assertTrue(isMinVersion("3.4", "3.5-RC1"));
        assertTrue(isMinVersion("3.4", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5.1"));
        assertTrue(isMinVersion("3.4", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5.1-EA"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.4.2", "3.5"));
        assertTrue(isMinVersion("3.4.2", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5-EA"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC1"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5.1"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-EA"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.5", "3.5"));
        assertTrue(isMinVersion("3.5", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5-EA"));
        assertTrue(isMinVersion("3.5", "3.5-RC"));
        assertTrue(isMinVersion("3.5", "3.5-RC1"));
        assertTrue(isMinVersion("3.5", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5.1"));
        assertTrue(isMinVersion("3.5", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5.1-EA"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-EA"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-EA"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC1-SNAPSHOT"));

        assertFalse(isMinVersion("3.5", "3.4"));
        assertFalse(isMinVersion("3.5", "3.4-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4-EA"));
        assertFalse(isMinVersion("3.5", "3.4-RC"));
        assertFalse(isMinVersion("3.5", "3.4-RC1"));
        assertFalse(isMinVersion("3.5", "3.4-RC1-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4.2"));
        assertFalse(isMinVersion("3.5", "3.4.2-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4.2-EA"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC1"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC1-SNAPSHOT"));

        assertFalse(isMinVersion("3.6", "3.5"));
        assertFalse(isMinVersion("3.6", "3.5-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5-EA"));
        assertFalse(isMinVersion("3.6", "3.5-RC"));
        assertFalse(isMinVersion("3.6", "3.5-RC1"));
        assertFalse(isMinVersion("3.6", "3.5-RC1-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5.1"));
        assertFalse(isMinVersion("3.6", "3.5.1-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5.1-EA"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC1"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC1-SNAPSHOT"));
    }
}
