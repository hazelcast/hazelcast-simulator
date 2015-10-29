package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterLayoutTest {

    @Test
    public void testGetVersionSpecs() {
        AgentData agentData = new AgentData(1, "127.0.0.1", "127.0.0.1");

        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        when(componentRegistry.getAgents()).thenReturn(singletonList(agentData));

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);
        when(workerParameters.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.OUT_OF_THE_BOX);

        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(1);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        ClusterLayout clusterLayout = new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        assertEquals(singleton(HazelcastJARs.OUT_OF_THE_BOX), clusterLayout.getVersionSpecs());
    }
}
