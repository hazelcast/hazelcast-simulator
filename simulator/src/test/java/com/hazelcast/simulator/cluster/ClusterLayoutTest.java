package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterLayoutTest {

    @Test
    public void testGetVersionSpecs() {
        AgentData agentData = new AgentData(1, "127.0.0.1", "127.0.0.1");

        testGetVersionSpecs(singletonList(agentData), 1, 0);
    }

    @Test
    public void testGetVersionSpecs_noWorkersOnSecondAgent() {
        AgentData agentData1 = new AgentData(1, "172.16.16.1", "127.0.0.1");
        AgentData agentData2 = new AgentData(2, "172.16.16.2", "127.0.0.1");

        List<AgentData> agents = new ArrayList<AgentData>(2);
        agents.add(agentData1);
        agents.add(agentData2);

        testGetVersionSpecs(agents, 1, 0);
    }

    private void testGetVersionSpecs(List<AgentData> agents, int memberCount, int clientCount) {
        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        when(componentRegistry.getAgents()).thenReturn(agents);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);
        when(workerParameters.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.OUT_OF_THE_BOX);

        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(memberCount);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(clientCount);

        ClusterLayout clusterLayout = new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        assertEquals(singleton(HazelcastJARs.OUT_OF_THE_BOX), clusterLayout.getVersionSpecs());
    }
}
