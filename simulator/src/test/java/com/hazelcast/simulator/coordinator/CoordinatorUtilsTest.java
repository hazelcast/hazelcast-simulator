package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.getMaxTestCaseIdLength;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.getPort;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorUtilsTest {

    private final CoordinatorParameters parameters = mock(CoordinatorParameters.class);
    private final ComponentRegistry componentRegistry = mock(ComponentRegistry.class);

    private int dedicatedMemberMachineCount = 0;
    private int memberWorkerCount = 0;
    private int clientWorkerCount = 0;

    private List<AgentMemberLayout> agentMemberLayouts;

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CoordinatorUtils.class);
    }

    @Test
    public void testGetPort() {
        String memberConfig = FileUtils.fileAsText("./dist/src/main/dist/conf/hazelcast.xml");

        CoordinatorParameters parameters = mock(CoordinatorParameters.class);
        when(parameters.getMemberHzConfig()).thenReturn(memberConfig);

        int port = getPort(parameters);
        assertEquals(5701, port);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetPort_withException() {
        CoordinatorParameters parameters = mock(CoordinatorParameters.class);
        when(parameters.getMemberHzConfig()).thenReturn("");

        getPort(parameters);
    }

    @Test
    public void testCreateAddressConfig() {
        List<AgentData> agents = new ArrayList<AgentData>();
        for (int i = 1; i <= 5; i++) {
            AgentData agentData = mock(AgentData.class);
            when(agentData.getPrivateAddress()).thenReturn("192.168.0." + i);
            agents.add(agentData);
        }

        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        when(componentRegistry.getAgents()).thenReturn(agents);

        String addressConfig = createAddressConfig("members", componentRegistry, 6666);
        for (int i = 1; i <= 5; i++) {
            assertTrue(addressConfig.contains("192.168.0." + i + ":6666"));
        }
    }

    @Test
    public void testMaxCaseIdLength() {
        List<TestCase> testCases = new ArrayList<TestCase>();
        testCases.add(new TestCase("abc"));
        testCases.add(new TestCase("88888888"));
        testCases.add(new TestCase(null));
        testCases.add(new TestCase("abcDEF"));
        testCases.add(new TestCase(""));
        testCases.add(new TestCase("four"));

        assertEquals(8, getMaxTestCaseIdLength(testCases));
    }

    @Test
    public void testInitMemberLayout_dedicatedMemberCountEqualsAgentCount() {
        dedicatedMemberMachineCount = 3;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MEMBER, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_dedicatedMemberCountHigherThanAgentCount() {
        dedicatedMemberMachineCount = 5;
        initMocks();

        initMemberLayout(componentRegistry, parameters);
    }

    @Test
    public void testInitMemberLayout_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        dedicatedMemberMachineCount = 2;
        clientWorkerCount = 1;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        dedicatedMemberMachineCount = 3;
        clientWorkerCount = 1;
        initMocks();

        initMemberLayout(componentRegistry, parameters);
    }

    @Test
    public void testInitMemberLayout_singleMemberWorker() {
        memberWorkerCount = 1;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testInitMemberLayout_memberWorkerOverflow() {
        memberWorkerCount = 4;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 1, 0);
    }

    @Test
    public void testInitMemberLayout_singleClientWorker() {
        clientWorkerCount = 1;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 1);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testClientWorkerOverflow() {
        clientWorkerCount = 5;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers1() {
        dedicatedMemberMachineCount = 1;
        memberWorkerCount = 2;
        clientWorkerCount = 3;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.CLIENT, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers2() {
        dedicatedMemberMachineCount = 2;
        memberWorkerCount = 2;
        clientWorkerCount = 3;
        initMocks();

        agentMemberLayouts = initMemberLayout(componentRegistry, parameters);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 3);
    }

    private void initMocks() {
        // ComponentRegistry
        List<AgentData> agents = new ArrayList<AgentData>(3);
        agents.add(new AgentData(1, "192.168.0.1", "192.168.0.1"));
        agents.add(new AgentData(2, "192.168.0.2", "192.168.0.2"));
        agents.add(new AgentData(3, "192.168.0.3", "192.168.0.3"));

        when(componentRegistry.getAgents()).thenReturn(agents);
        when(componentRegistry.agentCount()).thenReturn(3);

        // CoordinatorParameters
        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);

        when(parameters.getSimulatorProperties()).thenReturn(simulatorProperties);
        when(parameters.getProfiler()).thenReturn(JavaProfiler.NONE);
        when(parameters.getDedicatedMemberMachineCount()).thenReturn(dedicatedMemberMachineCount);
        when(parameters.getMemberWorkerCount()).thenReturn(memberWorkerCount);
        when(parameters.getClientWorkerCount()).thenReturn(clientWorkerCount);
    }

    private void assertAgentMemberLayout(int index, AgentMemberMode mode, int memberCount, int clientCount) {
        AgentMemberLayout layout = agentMemberLayouts.get(index);
        assertNotNull("Could not find AgentMemberLayout at index " + index, layout);

        String prefix = String.format("Agent %s members: %d clients: %d mode: %s",
                layout.getPublicAddress(),
                layout.getCount(WorkerType.MEMBER),
                layout.getCount(WorkerType.CLIENT),
                layout.getAgentMemberMode());

        assertEquals(prefix + " (agentMemberMode)", mode, layout.getAgentMemberMode());
        assertEquals(prefix + " (memberWorkerCount)", memberCount, layout.getCount(WorkerType.MEMBER));
        assertEquals(prefix + " (clientWorkerCount)", clientCount, layout.getCount(WorkerType.CLIENT));
    }
}
