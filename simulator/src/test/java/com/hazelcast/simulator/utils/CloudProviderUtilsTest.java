package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_GCE;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudProviderUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CloudProviderUtils.class);
    }

    @Test
    public void testIsStatic_true() throws Exception {
        assertTrue(CloudProviderUtils.isStatic(PROVIDER_STATIC));
    }

    @Test
    public void testIsStatic_false() throws Exception {
        assertFalse(CloudProviderUtils.isStatic(PROVIDER_GCE));
    }

    @Test
    public void testIsStatic_fromProperties_true() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_STATIC);

        assertTrue(CloudProviderUtils.isStatic(properties));
    }

    @Test
    public void testIsStatic_fromProperties_false() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_GCE);

        assertFalse(CloudProviderUtils.isStatic(properties));
    }

    @Test
    public void testIsEC2_true() throws Exception {
        assertTrue(CloudProviderUtils.isEC2(PROVIDER_EC2));
    }

    @Test
    public void testisEC2_false() throws Exception {
        assertFalse(CloudProviderUtils.isEC2(PROVIDER_GCE));
    }

    @Test
    public void testIsEC2_fromProperties_true() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_EC2);

        assertTrue(CloudProviderUtils.isEC2(properties));
    }

    @Test
    public void testIsEC2_fromProperties_false() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_GCE);

        assertFalse(CloudProviderUtils.isEC2(properties));
    }

    @Test
    public void testIsGCE_true() throws Exception {
        assertTrue(CloudProviderUtils.isGCE(PROVIDER_GCE));
    }

    @Test
    public void testisCGE_false() throws Exception {
        assertFalse(CloudProviderUtils.isGCE(PROVIDER_EC2));
    }

    @Test
    public void testIsGCE_fromProperties_true() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_GCE);

        assertTrue(CloudProviderUtils.isGCE(properties));
    }

    @Test
    public void testIsGCE_fromProperties_false() throws Exception {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(PROVIDER_STATIC);

        assertFalse(CloudProviderUtils.isGCE(properties));
    }
}
