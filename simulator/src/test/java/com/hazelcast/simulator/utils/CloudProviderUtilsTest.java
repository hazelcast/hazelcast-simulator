package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_GCE;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isCloudProvider;
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
    public void testIsCloudProvider_withLocal() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_LOCAL);

        assertFalse(isCloudProvider(properties));
    }

    @Test
    public void testIsCloudProvider_withStatic() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_STATIC);

        assertFalse(isCloudProvider(properties));
    }

    @Test
    public void testIsCloudProvider_withEC2() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_EC2);

        assertTrue(isCloudProvider(properties));
    }

    @Test
    public void testIsCloudProvider_withGCE() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_GCE);

        assertTrue(isCloudProvider(properties));
    }

    @Test
    public void testIsLocal_withLocal() {
        assertTrue(CloudProviderUtils.isLocal(PROVIDER_LOCAL));
    }

    @Test
    public void testIsLocal_withGCE() {
        assertFalse(CloudProviderUtils.isLocal(PROVIDER_GCE));
    }

    @Test
    public void testIsLocal_fromProperties_withLocal() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_LOCAL);

        assertTrue(CloudProviderUtils.isLocal(properties));
    }

    @Test
    public void testIsLocal_fromProperties_withGCE() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_GCE);

        assertFalse(CloudProviderUtils.isLocal(properties));
    }

    @Test
    public void testIsStatic_withStatic() {
        assertTrue(CloudProviderUtils.isStatic(PROVIDER_STATIC));
    }

    @Test
    public void testIsStatic_withGCE() {
        assertFalse(CloudProviderUtils.isStatic(PROVIDER_GCE));
    }

    @Test
    public void testIsStatic_fromProperties_withStatic() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_STATIC);

        assertTrue(CloudProviderUtils.isStatic(properties));
    }

    @Test
    public void testIsStatic_fromProperties_withGCE() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_GCE);

        assertFalse(CloudProviderUtils.isStatic(properties));
    }

    @Test
    public void testIsEC2_withEC2() {
        assertTrue(CloudProviderUtils.isEC2(PROVIDER_EC2));
    }

    @Test
    public void testisEC2_withGCE() {
        assertFalse(CloudProviderUtils.isEC2(PROVIDER_GCE));
    }

    @Test
    public void testIsEC2_fromProperties_withEC2() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_EC2);

        assertTrue(CloudProviderUtils.isEC2(properties));
    }

    @Test
    public void testIsEC2_fromProperties_withGCE() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_GCE);

        assertFalse(CloudProviderUtils.isEC2(properties));
    }

    @Test
    public void testIsGCE_withGCE() {
        assertTrue(CloudProviderUtils.isGCE(PROVIDER_GCE));
    }

    @Test
    public void testisCGE_withLocal() {
        assertFalse(CloudProviderUtils.isGCE(PROVIDER_LOCAL));
    }

    @Test
    public void testIsGCE_fromProperties_withGCE() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_GCE);

        assertTrue(CloudProviderUtils.isGCE(properties));
    }

    @Test
    public void testIsGCE_fromProperties_withLocal() {
        SimulatorProperties properties = getSimulatorProperties(PROVIDER_LOCAL);

        assertFalse(CloudProviderUtils.isGCE(properties));
    }

    private static SimulatorProperties getSimulatorProperties(String cloudProvider) {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(cloudProvider);
        return properties;
    }
}
