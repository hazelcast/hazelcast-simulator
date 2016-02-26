package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

    @Test(expected = CommandLineExitException.class)
    public void testGetFirstAgent_noAgents() {
        componentRegistry.getFirstAgent();
    }

    @Test
    public void testAddWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(10);

        assertEquals(0, componentRegistry.workerCount());
        componentRegistry.addWorkers(parentAddress, settingsList);

        assertEquals(10, componentRegistry.workerCount());
    }

    @Test
    public void testRemoveWorker_viaSimulatorAddress() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(5);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(5, componentRegistry.workerCount());

        componentRegistry.removeWorker(new SimulatorAddress(AddressLevel.WORKER, 1, 3, 0));
        assertEquals(4, componentRegistry.workerCount());
    }

    @Test
    public void testRemoveWorker_viaWorkerData() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(5);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(5, componentRegistry.workerCount());

        componentRegistry.removeWorker(componentRegistry.getWorkers().get(0));
        assertEquals(4, componentRegistry.workerCount());
    }

    @Test
    public void testHasClientWorkers_withoutClientWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(2);
        componentRegistry.addWorkers(parentAddress, settingsList);

        assertFalse(componentRegistry.hasClientWorkers());
    }

    @Test
    public void testHasClientWorkers_withClientWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(2);
        componentRegistry.addWorkers(parentAddress, settingsList);

        settingsList = getWorkerJvmSettingsList(2, WorkerType.CLIENT);
        componentRegistry.addWorkers(parentAddress, settingsList);

        assertTrue(componentRegistry.hasClientWorkers());
    }

    @Test
    public void testGetWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
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
    public void testGetWorkers_withTargetType() {
        componentRegistry.addAgent("172.16.16.1", "127.0.0.1");
        componentRegistry.addAgent("172.16.16.2", "127.0.0.1");
        componentRegistry.addAgent("172.16.16.3", "127.0.0.1");
        assertEquals(3, componentRegistry.agentCount());

        for (AgentData agentData : componentRegistry.getAgents()) {
            List<WorkerJvmSettings> memberSettings = getWorkerJvmSettingsList(1, WorkerType.MEMBER);
            List<WorkerJvmSettings> clientSettings = getWorkerJvmSettingsList(1, WorkerType.CLIENT);

            componentRegistry.addWorkers(agentData.getAddress(), memberSettings);
            componentRegistry.addWorkers(agentData.getAddress(), clientSettings);
            componentRegistry.addWorkers(agentData.getAddress(), memberSettings);
            componentRegistry.addWorkers(agentData.getAddress(), clientSettings);
            componentRegistry.addWorkers(agentData.getAddress(), memberSettings);
            componentRegistry.addWorkers(agentData.getAddress(), clientSettings);
        }
        assertEquals(18, componentRegistry.workerCount());

        List<String> workers = componentRegistry.getWorkerAddresses(TargetType.ALL, 0);
        assertEquals(0, workers.size());

        workers = componentRegistry.getWorkerAddresses(TargetType.ALL, 12);
        assertEquals(12, workers.size());

        workers = componentRegistry.getWorkerAddresses(TargetType.ALL, 8);
        assertEquals(8, workers.size());

        List<WorkerData> workerDataList = componentRegistry.getWorkers(TargetType.MEMBER, 0);
        assertEquals(0, workerDataList.size());

        workerDataList = componentRegistry.getWorkers(TargetType.MEMBER, 7);
        assertEquals(7, workerDataList.size());
        for (WorkerData workerData : workerDataList) {
            assertTrue(workerData.isMemberWorker());
        }

        workerDataList = componentRegistry.getWorkers(TargetType.CLIENT, 0);
        assertEquals(0, workerDataList.size());

        workerDataList = componentRegistry.getWorkers(TargetType.CLIENT, 7);
        assertEquals(7, workerDataList.size());
        for (WorkerData workerData : workerDataList) {
            assertFalse(workerData.isMemberWorker());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWorkers_withHigherWorkerCountThanRegisteredWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.MEMBER));
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.CLIENT));
        assertEquals(4, componentRegistry.workerCount());

        componentRegistry.getWorkerAddresses(TargetType.ALL, 5);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWorkers_getClientWorkers_notEnoughWorkersFound() {
        SimulatorAddress parentAddress = getSingleAgent();
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.MEMBER));
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.CLIENT));
        assertEquals(4, componentRegistry.workerCount());

        componentRegistry.getWorkerAddresses(TargetType.CLIENT, 3);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWorkers_getMemberWorkers_notEnoughWorkersFound() {
        SimulatorAddress parentAddress = getSingleAgent();
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.MEMBER));
        componentRegistry.addWorkers(parentAddress, getWorkerJvmSettingsList(2, WorkerType.CLIENT));
        assertEquals(4, componentRegistry.workerCount());

        componentRegistry.getWorkerAddresses(TargetType.MEMBER, 3);
    }

    @Test
    public void testGetFirstWorker() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(2);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(2, componentRegistry.workerCount());

        WorkerData workerData = componentRegistry.getFirstWorker();
        assertEquals(1, workerData.getAddress().getWorkerIndex());
        assertEquals(AddressLevel.WORKER, workerData.getAddress().getAddressLevel());

        assertEquals(1, workerData.getSettings().getWorkerIndex());
        assertEquals(WorkerType.MEMBER, workerData.getSettings().getWorkerType());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetFirstWorker_noWorkers() {
        componentRegistry.getFirstWorker();
    }

    @Test
    public void testGetMissingWorkers() {
        SimulatorAddress parentAddress = getSingleAgent();
        List<WorkerJvmSettings> settingsList = getWorkerJvmSettingsList(5);

        componentRegistry.addWorkers(parentAddress, settingsList);
        assertEquals(5, componentRegistry.workerCount());

        Set<SimulatorAddress> finishedWorkers = new HashSet<SimulatorAddress>();
        for (WorkerJvmSettings workerJvmSettings : settingsList) {
            SimulatorAddress workerAddress = parentAddress.getChild(workerJvmSettings.getWorkerIndex());
            finishedWorkers.add(workerAddress);
            if (finishedWorkers.size() == 3) {
                break;
            }
        }

        Set<SimulatorAddress> missingWorkers = componentRegistry.getMissingWorkers(finishedWorkers);
        assertEquals(2, missingWorkers.size());
    }

    @Test
    public void testAddTests() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        testSuite.addTest(new TestCase("Test2"));
        testSuite.addTest(new TestCase("Test3"));

        componentRegistry.addTests(testSuite);

        assertEquals(3, componentRegistry.testCount());
    }

    @Test
    public void testRemoveTests() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        componentRegistry.addTests(testSuite);

        componentRegistry.removeTests();

        assertEquals(0, componentRegistry.testCount());
    }

    @Test
    public void testGetTests() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        testSuite.addTest(new TestCase("Test2"));
        testSuite.addTest(new TestCase("Test3"));
        componentRegistry.addTests(testSuite);

        Collection<TestData> tests = componentRegistry.getTests();

        assertEquals(3, tests.size());
        for (TestData testData : tests) {
            assertTrue(testData.getTestCase().getId().startsWith("Test"));
            assertEquals(AddressLevel.TEST, testData.getAddress().getAddressLevel());
        }
    }

    @Test
    public void testGetTest() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        testSuite.addTest(new TestCase("Test2"));
        testSuite.addTest(new TestCase("Test3"));
        componentRegistry.addTests(testSuite);

        TestData testData = componentRegistry.getTest("Test2");

        assertEquals(2, testData.getTestIndex());
        assertEquals(AddressLevel.TEST, testData.getAddress().getAddressLevel());
        assertEquals("Test2", testData.getTestCase().getId());
    }

    private SimulatorAddress getSingleAgent() {
        componentRegistry.addAgent("172.16.16.1", "127.0.0.1");
        return componentRegistry.getFirstAgent().getAddress();
    }

    private List<WorkerJvmSettings> getWorkerJvmSettingsList(int workerCount) {
        return getWorkerJvmSettingsList(workerCount, WorkerType.MEMBER);
    }

    private List<WorkerJvmSettings> getWorkerJvmSettingsList(int workerCount, WorkerType workerType) {
        List<WorkerJvmSettings> settingsList = new ArrayList<WorkerJvmSettings>();
        for (int i = 1; i <= workerCount; i++) {
            WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
            when(workerJvmSettings.getWorkerIndex()).thenReturn(i);
            when(workerJvmSettings.getWorkerType()).thenReturn(workerType);

            settingsList.add(workerJvmSettings);
        }
        return settingsList;
    }
}
