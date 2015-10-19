package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ComponentRegistryTest {

    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    @Test
    public void testAddAgent() {
        assertEquals(0, componentRegistry.agentCount());

        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, componentRegistry.agentCount());
    }

    @Test
    public void testRemoveAgent() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, componentRegistry.agentCount());

        AgentData agentData = componentRegistry.getFirstAgent();
        componentRegistry.removeAgent(agentData);
        assertEquals(0, componentRegistry.agentCount());
    }

    @Test
    public void testGetAgents() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");

        assertEquals(2, componentRegistry.agentCount());
        assertEquals(2, componentRegistry.getAgents().size());
    }

    @Test
    public void testGetAgents_withCount() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");
        assertEquals(3, componentRegistry.agentCount());

        List<AgentData> agents = componentRegistry.getAgents(1);
        assertEquals(1, agents.size());
        assertEquals("192.168.0.3", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(0).getPrivateAddress());

        agents = componentRegistry.getAgents(2);
        assertEquals(2, agents.size());
        assertEquals("192.168.0.2", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.2", agents.get(0).getPrivateAddress());
        assertEquals("192.168.0.3", agents.get(1).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(1).getPrivateAddress());
    }

    @Test
    public void testAddWorkers() {
        SimulatorAddress parentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(10);

        assertEquals(0, componentRegistry.workerCount());
        componentRegistry.addWorkers(parentAddress, settingsList);

        assertEquals(10, componentRegistry.workerCount());
    }

    @Test
    public void testRemoveWorker() {
        SimulatorAddress parentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(5);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(5, componentRegistry.workerCount());

        componentRegistry.removeWorker(componentRegistry.getWorkers().get(0));
        assertEquals(4, componentRegistry.workerCount());
    }

    @Test
    public void testGetWorkers() {
        SimulatorAddress parentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(10);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(10, componentRegistry.workerCount());

        List<WorkerData> workers = componentRegistry.getWorkers();
        for (int i = 0; i < 10; i++) {
            WorkerData workerData = workers.get(i);
            assertEquals(i + 1, workerData.getAddress().getWorkerIndex());
            assertEquals(AddressLevel.WORKER, workerData.getAddress().getAddressLevel());

            assertEquals(i + 1, workerData.getSettings().getWorkerIndex());
            assertEquals(WorkerType.MEMBER, workerData.getSettings().getWorkerType());
        }
    }

    @Test
    public void testGetFirstWorker() {
        SimulatorAddress parentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(2);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(2, componentRegistry.workerCount());

        WorkerData workerData = componentRegistry.getFirstWorker();
        assertEquals(1, workerData.getAddress().getWorkerIndex());
        assertEquals(AddressLevel.WORKER, workerData.getAddress().getAddressLevel());

        assertEquals(1, workerData.getSettings().getWorkerIndex());
        assertEquals(WorkerType.MEMBER, workerData.getSettings().getWorkerType());
    }

    @Test
    public void testGetMissingWorkers() {
        SimulatorAddress parentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(5);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(5, componentRegistry.workerCount());

        Set<String> finishedWorkers = new HashSet<String>();
        for (WorkerJvmSettings workerJvmSettings : settingsList) {
            SimulatorAddress workerAddress = parentAddress.getChild(workerJvmSettings.getWorkerIndex());
            finishedWorkers.add(workerAddress.toString());
            if (finishedWorkers.size() == 3) {
                break;
            }
        }

        Set<String> missingWorkers = componentRegistry.getMissingWorkers(finishedWorkers);
        assertEquals(2, missingWorkers.size());
    }

    private List<WorkerJvmSettings> getWorkerJvmSettingsList(int workerCount) {
        List<WorkerJvmSettings> settingsList = new ArrayList<WorkerJvmSettings>();
        for (int i = 1; i <= workerCount; i++) {
            WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
            when(workerJvmSettings.getWorkerIndex()).thenReturn(i);
            when(workerJvmSettings.getWorkerType()).thenReturn(WorkerType.MEMBER);

            settingsList.add(workerJvmSettings);
        }
        return settingsList;
    }
}
