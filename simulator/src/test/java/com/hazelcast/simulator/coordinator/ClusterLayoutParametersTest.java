package com.hazelcast.simulator.coordinator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterLayoutParametersTest {

    @Test
    public void testConstructor() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(1, 3, 5);

        assertEquals(1, clusterLayoutParameters.getDedicatedMemberMachineCount());
        assertEquals(3, clusterLayoutParameters.getClientWorkerCount());
        assertEquals(5, clusterLayoutParameters.getMemberWorkerCount());
    }

    @Test
    public void testInitMemberCount() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(1, 3, -1);
        assertEquals(-1, clusterLayoutParameters.getMemberWorkerCount());

        clusterLayoutParameters.initMemberWorkerCount(10);
        assertEquals(10, clusterLayoutParameters.getMemberWorkerCount());
    }
}
