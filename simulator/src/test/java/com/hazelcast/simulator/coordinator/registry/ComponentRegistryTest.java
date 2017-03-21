package com.hazelcast.simulator.coordinator.registry;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.TargetType;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.coordinator.registry.AgentData.AgentWorkerMode;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ComponentRegistryTest {

    private final ComponentRegistry registry = new ComponentRegistry();

    @Test(expected = CommandLineExitException.class)
    public void test_assignDedicatedMemberMachines_whenDedicatedMemberCountNegative() {
        registry.assignDedicatedMemberMachines(-1);
    }

    @Test(expected = CommandLineExitException.class)
    public void test_assignDedicatedMemberMachines_whenDedicatedMemberCountHigherThanAgentCount() {
        registry.addAgent("192.168.0.1", "192.168.0.1");

        registry.assignDedicatedMemberMachines(2);
    }

    @Test
    public void test_assignDedicatedMemberMachines() {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");
        registry.addAgent("192.168.0.3", "192.168.0.3");

        registry.assignDedicatedMemberMachines(2);

        assertEquals(AgentWorkerMode.MEMBERS_ONLY, registry.getAgents().get(0).getAgentWorkerMode());
        assertEquals(AgentWorkerMode.MEMBERS_ONLY, registry.getAgents().get(1).getAgentWorkerMode());
        assertEquals(AgentWorkerMode.CLIENTS_ONLY, registry.getAgents().get(2).getAgentWorkerMode());
    }

    @Test
    public void testAddAgent() {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, registry.agentCount());
    }

    @Test
    public void testRemoveAgent() {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        registry.removeAgent(agentData);
        assertEquals(0, registry.agentCount());
    }

    @Test
    public void testGetAgents() {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");

        assertEquals(2, registry.agentCount());
        assertEquals(2, registry.getAgents().size());
    }

    @Test
    public void testGetAgents_withCount() {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");
        registry.addAgent("192.168.0.3", "192.168.0.3");
        assertEquals(3, registry.agentCount());

        List<AgentData> agents = registry.getAgents(1);
        assertEquals(1, agents.size());
        assertEquals("192.168.0.3", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(0).getPrivateAddress());

        agents = registry.getAgents(2);
        assertEquals(2, agents.size());
        assertEquals("192.168.0.2", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.2", agents.get(0).getPrivateAddress());
        assertEquals("192.168.0.3", agents.get(1).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(1).getPrivateAddress());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetFirstAgent_noAgents() {
        registry.getFirstAgent();
    }

    @Test
    public void testAddWorkers() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 10);

        assertEquals(0, registry.workerCount());
        registry.addWorkers(parametersList);

        assertEquals(10, registry.workerCount());
    }

    @Test
    public void testRemoveWorker_viaSimulatorAddress() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 5);

        registry.addWorkers(parametersList);
        assertEquals(5, registry.workerCount());

        registry.removeWorker(new SimulatorAddress(WORKER, 1, 3, 0));
        assertEquals(4, registry.workerCount());
    }

    @Test
    public void testRemoveWorker_viaWorkerData() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 5);

        registry.addWorkers(parametersList);
        assertEquals(5, registry.workerCount());

        registry.removeWorker(registry.getWorkers().get(0));
        assertEquals(4, registry.workerCount());
    }

    @Test
    public void testHasClientWorkers_withoutClientWorkers() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 2);
        registry.addWorkers(parametersList);

        assertFalse(registry.hasClientWorkers());
    }

    @Test
    public void testHasClientWorkers_withClientWorkers() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 2);
        registry.addWorkers(parametersList);

        parametersList = newWorkerParametersList(agentAddress, 2, "javaclient");
        registry.addWorkers(parametersList);

        assertTrue(registry.hasClientWorkers());
    }

    @Test
    public void testGetWorkers() {
        SimulatorAddress agentAddress = addAgent();
        List<WorkerParameters> parametersList = newWorkerParametersList(agentAddress, 10);

        registry.addWorkers(parametersList);
        assertEquals(10, registry.workerCount());

        List<WorkerData> workers = registry.getWorkers();
        for (int i = 0; i < 10; i++) {
            WorkerData workerData = workers.get(i);
            assertEquals(i + 1, workerData.getAddress().getWorkerIndex());
            assertEquals(WORKER, workerData.getAddress().getAddressLevel());

            //assertEquals(i + 1, workerData.getParameters().getWorkerIndex());
            assertEquals("member", workerData.getParameters().getWorkerType());
        }
    }

    @Test
    public void testGetWorkers_withTargetType() {
        registry.addAgent("172.16.16.1", "127.0.0.1");
        registry.addAgent("172.16.16.2", "127.0.0.1");
        registry.addAgent("172.16.16.3", "127.0.0.1");

        for (AgentData agentData : registry.getAgents()) {
            List<WorkerParameters> memberSettings = newWorkerParametersList(agentData.getAddress(), 1, "member");
            List<WorkerParameters> clientSettings = newWorkerParametersList(agentData.getAddress(), 1, "javaclient");

            registry.addWorkers(memberSettings);
            registry.addWorkers(clientSettings);
            registry.addWorkers(memberSettings);
            registry.addWorkers(clientSettings);
            registry.addWorkers(memberSettings);
            registry.addWorkers(clientSettings);
        }
        assertEquals(18, registry.workerCount());

        List<String> workers = registry.getWorkerAddresses(TargetType.ALL, 0);
        assertEquals(0, workers.size());

        workers = registry.getWorkerAddresses(TargetType.ALL, 12);
        assertEquals(12, workers.size());

        workers = registry.getWorkerAddresses(TargetType.ALL, 8);
        assertEquals(8, workers.size());

        List<WorkerData> workerDataList = registry.getWorkers(TargetType.MEMBER, 0);
        assertEquals(0, workerDataList.size());

        workerDataList = registry.getWorkers(TargetType.MEMBER, 7);
        assertEquals(7, workerDataList.size());
        for (WorkerData workerData : workerDataList) {
            assertTrue(workerData.isMemberWorker());
        }

        workerDataList = registry.getWorkers(TargetType.CLIENT, 0);
        assertEquals(0, workerDataList.size());

        workerDataList = registry.getWorkers(TargetType.CLIENT, 7);
        assertEquals(7, workerDataList.size());
        for (WorkerData workerData : workerDataList) {
            assertFalse(workerData.isMemberWorker());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetWorkers_withHigherWorkerCountThanRegisteredWorkers() {
        SimulatorAddress agentAddress = addAgent();
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "member"));
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "javaclient"));
        assertEquals(4, registry.workerCount());

        registry.getWorkerAddresses(TargetType.ALL, 5);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWorkers_getClientWorkers_notEnoughWorkersFound() {
        SimulatorAddress agentAddress = addAgent();
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "member"));
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "javaclient"));
        assertEquals(4, registry.workerCount());

        registry.getWorkerAddresses(TargetType.CLIENT, 3);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetWorkers_getMemberWorkers_notEnoughWorkersFound() {
        SimulatorAddress agentAddress = addAgent();
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "member"));
        registry.addWorkers(newWorkerParametersList(agentAddress, 2, "javaclient"));
        assertEquals(4, registry.workerCount());

        registry.getWorkerAddresses(TargetType.MEMBER, 3);
    }

    @Test
    public void testAddTests() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        testSuite.addTest(new TestCase("Test2"));
        testSuite.addTest(new TestCase("Test3"));

        List<TestData> tests = registry.addTests(testSuite);

        assertEquals(3, tests.size());
        assertEquals(3, registry.testCount());
    }

    @Test
    public void testAddTests_testIdFixing() {
        TestCase test1 = new TestCase("foo");
        TestCase test2 = new TestCase("foo");
        TestCase test3 = new TestCase("foo");

        registry.addTests(new TestSuite().addTest(test1));
        registry.addTests(new TestSuite().addTest(test2));
        registry.addTests(new TestSuite().addTest(test3));

        assertEquals("foo", test1.getId());
        assertEquals("foo__1", test2.getId());
        assertEquals("foo__2", test3.getId());
    }

//    @Test
//    public void testRemoveTests() {
//        TestSuite testSuite1 = new TestSuite()
//                .addTest(new TestCase("Test1a"))
//                .addTest(new TestCase("Test1b"));
//        registry.addTests(testSuite1);
//
//        TestSuite testSuite2 = new TestSuite()
//                .addTest(new TestCase("Test2a"))
//                .addTest(new TestCase("Test2b"));
//        registry.addTests(testSuite2);
//
//        registry.removeTests(testSuite1);
//
//        assertEquals(2, registry.testCount());
//        for (TestData testData : registry.getTests()) {
//            assertSame(testSuite2, testData.getTestSuite());
//        }
//    }

    @Test
    public void testGetTests() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("Test1"));
        testSuite.addTest(new TestCase("Test2"));
        testSuite.addTest(new TestCase("Test3"));
        registry.addTests(testSuite);

        Collection<TestData> tests = registry.getTests();

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
        registry.addTests(testSuite);

        TestData testData = registry.getTest("Test2");

        assertEquals(2, testData.getTestIndex());
        assertEquals(AddressLevel.TEST, testData.getAddress().getAddressLevel());
        assertEquals("Test2", testData.getTestCase().getId());
    }

    private SimulatorAddress addAgent() {
        registry.addAgent("172.16.16.1", "127.0.0.1");
        return registry.getFirstAgent().getAddress();
    }

    private List<WorkerParameters> newWorkerParametersList(SimulatorAddress agent, int workerCount) {
        return newWorkerParametersList(agent, workerCount, "member");
    }

    private List<WorkerParameters> newWorkerParametersList(SimulatorAddress agent, int workerCount, String workerType) {
        List<WorkerParameters> result = new ArrayList<WorkerParameters>();
        for (int k = 1; k <= workerCount; k++) {
            result.add(new WorkerParameters()
                    .set("WORKER_TYPE", workerType)
                    .set("WORKER_INDEX", k )
                    .set("WORKER_ADDRESS", new SimulatorAddress(WORKER, agent.getAgentIndex(), k , 0)));
        }
        return result;
    }
}
