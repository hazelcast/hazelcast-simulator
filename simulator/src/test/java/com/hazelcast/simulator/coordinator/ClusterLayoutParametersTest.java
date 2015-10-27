package com.hazelcast.simulator.coordinator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterLayoutParametersTest {

    @Test
    public void testConstructor() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, 1, 3, 5, 10);

        assertNull(clusterLayoutParameters.getClusterConfiguration());
        assertEquals(1, clusterLayoutParameters.getMemberWorkerCount());
        assertEquals(3, clusterLayoutParameters.getClientWorkerCount());
        assertEquals(5, clusterLayoutParameters.getDedicatedMemberMachineCount());
    }

    @Test
    public void testConstructor_withNegativeMemberCount() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, -1, 3, 5, 10);

        assertNull(clusterLayoutParameters.getClusterConfiguration());
        assertEquals(10, clusterLayoutParameters.getMemberWorkerCount());
        assertEquals(3, clusterLayoutParameters.getClientWorkerCount());
        assertEquals(5, clusterLayoutParameters.getDedicatedMemberMachineCount());
    }
}
