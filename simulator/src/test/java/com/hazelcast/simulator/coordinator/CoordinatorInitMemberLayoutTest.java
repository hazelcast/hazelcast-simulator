package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorInitMemberLayoutTest {

    @Mock
    private final WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();

    @Mock
    private final AgentsClient agentsClient = mock(AgentsClient.class);

    @InjectMocks
    private Coordinator coordinator;

    private List<AgentMemberLayout> agentMemberLayouts;

    @Before
    public void setUp() {
        System.setProperty("user.dir", "./dist/src/main/dist");

        MockitoAnnotations.initMocks(this);

        List<String> privateAddressList = new ArrayList<String>(3);
        privateAddressList.add("192.168.0.1");
        privateAddressList.add("192.168.0.2");
        privateAddressList.add("192.168.0.3");

        when(agentsClient.getPublicAddresses()).thenReturn(privateAddressList);
        when(agentsClient.getAgentCount()).thenReturn(3);
    }

    @Test
    public void testDedicatedMemberCountEqualsAgentCount() {
        coordinator.dedicatedMemberMachineCount = 3;

        coordinator.initMemberLayout();
    }

    @Test(expected = CommandLineExitException.class)
    public void testDedicatedMemberCountHigherThanAgentCount() {
        coordinator.dedicatedMemberMachineCount = 5;

        coordinator.initMemberLayout();
    }

    @Test()
    public void testAgentCountSufficientForDedicatedMembersAndClientWorkers() {
        coordinator.dedicatedMemberMachineCount = 2;
        workerJvmSettings.clientWorkerCount = 1;

        coordinator.initMemberLayout();
    }

    @Test(expected = CommandLineExitException.class)
    public void testAgentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        coordinator.dedicatedMemberMachineCount = 3;
        workerJvmSettings.clientWorkerCount = 1;

        coordinator.initMemberLayout();
    }

    @Test
    public void testSingleMemberWorker() {
        workerJvmSettings.memberWorkerCount = 1;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testMemberWorkerOverflow() {
        workerJvmSettings.memberWorkerCount = 4;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 1, 0);
    }

    @Test
    public void testSingleClientWorker() {
        workerJvmSettings.clientWorkerCount = 1;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 1);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testClientWorkerOverflow() {
        workerJvmSettings.clientWorkerCount = 5;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 1);
    }

    @Test
    public void testDedicatedAndMixedWorkers1() {
        coordinator.dedicatedMemberMachineCount = 1;
        workerJvmSettings.memberWorkerCount = 2;
        workerJvmSettings.clientWorkerCount = 3;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.CLIENT, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 1);
    }

    @Test
    public void testDedicatedAndMixedWorkers2() {
        coordinator.dedicatedMemberMachineCount = 2;
        workerJvmSettings.memberWorkerCount = 2;
        workerJvmSettings.clientWorkerCount = 3;

        agentMemberLayouts = coordinator.initMemberLayout();
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 3);
    }

    private void assertAgentMemberLayout(int index, AgentMemberMode mode, int memberCount, int clientCount) {
        AgentMemberLayout layout = agentMemberLayouts.get(index);
        assertNotNull("Could not find AgentMemberLayout at index " + index, layout);

        String prefix = String.format("Agent %s members: %d clients: %d mode: %s",
                layout.publicIp,
                layout.memberSettings.memberWorkerCount,
                layout.clientSettings.clientWorkerCount,
                layout.agentMemberMode);

        assertEquals(prefix + " (agentMemberMode)", mode, layout.agentMemberMode);
        assertEquals(prefix + " (memberSettings.memberWorkerCount)", memberCount, layout.memberSettings.memberWorkerCount);
        assertEquals(prefix + " (memberSettings.clientWorkerCount)", 0, layout.memberSettings.clientWorkerCount);
        assertEquals(prefix + " (clientSettings.memberWorkerCount)", 0, layout.clientSettings.memberWorkerCount);
        assertEquals(prefix + " (clientSettings.clientWorkerCount)", clientCount, layout.clientSettings.clientWorkerCount);
    }
}
