package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CloudProviderUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CloudProviderUtils.class);
    }

    @Test
    public void testIsStatic_true() throws Exception {
        assertTrue(CloudProviderUtils.isStatic("static"));
    }

    @Test
    public void testIsStatic_false() throws Exception {
        assertFalse(CloudProviderUtils.isStatic("google-compute-engine"));
    }

    @Test
    public void testIsEC2_true() throws Exception {
        assertTrue(CloudProviderUtils.isEC2("aws-ec2"));
    }

    @Test
    public void testisEC2_false() throws Exception {
        assertFalse(CloudProviderUtils.isEC2("google-compute-engine"));
    }
}
