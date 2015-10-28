package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class ClusterLayoutParametersTest {

    private WorkerConfigurationConverter converter = mock(WorkerConfigurationConverter.class);

    @Test
    public void testConstructor() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, converter, 1, 3, 5, 10);

        assertNull(clusterLayoutParameters.getClusterConfiguration());
        assertEquals(converter, clusterLayoutParameters.getWorkerConfigurationConverter());
        assertEquals(1, clusterLayoutParameters.getMemberWorkerCount());
        assertEquals(3, clusterLayoutParameters.getClientWorkerCount());
        assertEquals(5, clusterLayoutParameters.getDedicatedMemberMachineCount());
    }

    @Test
    public void testConstructor_withNegativeMemberCount() {
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, converter, -1, 3, 5, 10);

        assertNull(clusterLayoutParameters.getClusterConfiguration());
        assertEquals(converter, clusterLayoutParameters.getWorkerConfigurationConverter());
        assertEquals(10, clusterLayoutParameters.getMemberWorkerCount());
        assertEquals(3, clusterLayoutParameters.getClientWorkerCount());
        assertEquals(5, clusterLayoutParameters.getDedicatedMemberMachineCount());
    }
}
