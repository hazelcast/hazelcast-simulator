package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.BuildInfoUtils.DEFAULT_MINOR_VERSION;
import static com.hazelcast.simulator.utils.BuildInfoUtils.getMinorVersion;
import static com.hazelcast.simulator.utils.BuildInfoUtils.isMinVersion;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuildInfoUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(BuildInfoUtils.class);
    }

    @Test
    public void testGetMinorVersion() {
        assertEquals(6, getMinorVersion());
    }

    @Test
    public void testGetMinorVersion_whenVersionCannotBeFound_thenReturnDefaultVersion() {
        assertEquals(DEFAULT_MINOR_VERSION, getMinorVersion(null));
    }

    @Test
    public void testIsMinVersion() {
        assertTrue(isMinVersion("3.6"));
    }

    @Test
    public void testIsMinVersion_whenVersionCannotBeFound_thenReturnFalse() {
        assertFalse(isMinVersion("3.6", null));
    }
}
