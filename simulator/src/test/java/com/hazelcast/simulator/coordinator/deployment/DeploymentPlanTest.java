package com.hazelcast.simulator.coordinator.deployment;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.coordinator.deployment.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentPlan.generateFromArguments;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentPlan.generateFromXml;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentPlanTest {
    private final Map<WorkerType,WorkerParameters> workerParametersMap = new HashMap<WorkerType, WorkerParameters>();
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private SimulatorAddress firstAgent;
    private SimulatorAddress secondAgent;
    private SimulatorAddress thirdAgent;

    private Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment;
    private WorkerConfigurationConverter converter;

    @Before
    public void setUp() {
        workerParametersMap.put(WorkerType.MEMBER,  mock(WorkerParameters.class));
        workerParametersMap.put(WorkerType.CLIENT,  mock(WorkerParameters.class));

        firstAgent = componentRegistry.addAgent("192.168.0.1", "192.168.0.1").getAddress();
        secondAgent = componentRegistry.addAgent("192.168.0.2", "192.168.0.2").getAddress();
        thirdAgent = componentRegistry.addAgent("192.168.0.3", "192.168.0.3").getAddress();

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        converter = new WorkerConfigurationConverter(5701, "defaultLicenseKey", workerParametersMap, simulatorProperties,
                componentRegistry);
    }

    @Test
    public void testFormatIpAddresses_sameAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "192.168.0.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = DeploymentPlan.formatIpAddresses(agentWorkerLayout);
        assertTrue(ipAddresses.contains("192.168.0.1"));
    }

    @Test
    public void testFormatIpAddresses_differentAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "172.16.16.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = DeploymentPlan.formatIpAddresses(agentWorkerLayout);
        assertTrue(ipAddresses.contains("192.168.0.1"));
        assertTrue(ipAddresses.contains("172.16.16.1"));
    }

    @Test
    public void testGenerateFromXml() {
        String xml = "<clusterConfiguration>"
                + NEW_LINE + "  <workerConfiguration name=\"memberWorker\" type=\"MEMBER\"/>"
                + NEW_LINE + "  <workerConfiguration name=\"clientWorker\" type=\"CLIENT\"/>"
                + NEW_LINE + "  <nodeConfiguration>"
                + NEW_LINE + "    <workerGroup configuration=\"memberWorker\" count=\"1\"/>"
                + NEW_LINE + "  </nodeConfiguration>"
                + NEW_LINE + "  <nodeConfiguration>"
                + NEW_LINE + "    <workerGroup configuration=\"clientWorker\" count=\"2\"/>"
                + NEW_LINE + "  </nodeConfiguration>"
                + NEW_LINE + "  <nodeConfiguration>"
                + NEW_LINE + "    <workerGroup configuration=\"memberWorker\" count=\"3\"/>"
                + NEW_LINE + "    <workerGroup configuration=\"clientWorker\" count=\"4\"/>"
                + NEW_LINE + "  </nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        workerDeployment = generateFromXml(componentRegistry, workerParametersMap, converter, xml);
        assertWorkerDeployment(firstAgent, 1, 0);
        assertWorkerDeployment(secondAgent, 0, 2);
        assertWorkerDeployment(thirdAgent, 3, 4);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromXml_countMismatch() {
        String xml = "<clusterConfiguration>"
                + NEW_LINE + "  <workerConfiguration name=\"memberWorker\" type=\"MEMBER\"/>"
                + NEW_LINE + "  <workerConfiguration name=\"clientWorker\" type=\"CLIENT\"/>"
                + NEW_LINE + "  <nodeConfiguration>"
                + NEW_LINE + "    <workerGroup configuration=\"memberWorker\" count=\"1\"/>"
                + NEW_LINE + "  </nodeConfiguration>"
                + NEW_LINE + "  <nodeConfiguration>"
                + NEW_LINE + "    <workerGroup configuration=\"clientWorker\" count=\"2\"/>"
                + NEW_LINE + "  </nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        generateFromXml(componentRegistry, workerParametersMap, converter, xml);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromXml_hzConfigFileNotExists() {
        String xml = "<clusterConfiguration>"
                + NEW_LINE + "\t<workerConfiguration name=\"memberWorker\" type=\"MEMBER\" hzConfigFile=\"notExists\"/>"
                + NEW_LINE + "\t<nodeConfiguration>"
                + NEW_LINE + "\t<workerGroup configuration=\"memberWorker\" count=\"1\"/>"
                + NEW_LINE + "\t</nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        generateFromXml(componentRegistry, workerParametersMap, converter, xml);
    }

    @Test
    public void testGenerateFromArguments_dedicatedMemberCountEqualsAgentCount() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 1, 0, 3);

        assertWorkerDeployment(firstAgent, 1, 0);
        assertWorkerDeployment(secondAgent, 0, 0);
        assertWorkerDeployment(thirdAgent, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noAgents() {
        generateFromArguments(new ComponentRegistry(), workerParametersMap, 0, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountNegative() {
        generateFromArguments(componentRegistry, workerParametersMap, 0, 0, -1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountHigherThanAgentCount() {
        generateFromArguments(componentRegistry, workerParametersMap, 1, 0, 5);
    }

    @Test
    public void testGenerateFromArguments_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 0, 1, 2);

        assertWorkerDeployment(firstAgent, 0, 0);
        assertWorkerDeployment(secondAgent, 0, 0);
        assertWorkerDeployment(thirdAgent, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        generateFromArguments(componentRegistry, workerParametersMap, 0, 1, 3);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noWorkersDefined() {
        generateFromArguments(componentRegistry, workerParametersMap, 0, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_singleMemberWorker() {
        //  when(workerParameters.getPerformanceMonitorIntervalSeconds()).thenReturn(10);

        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 1, 0, 0);

        assertWorkerDeployment(firstAgent, 1, 0);
        assertWorkerDeployment(secondAgent, 0, 0);
        assertWorkerDeployment(thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_memberWorkerOverflow() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 4, 0, 0);

        assertWorkerDeployment(firstAgent, 2, 0);
        assertWorkerDeployment(secondAgent, 1, 0);
        assertWorkerDeployment(thirdAgent, 1, 0);
    }

    @Test
    public void testGenerateFromArguments_singleClientWorker() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 0, 1, 0);

        assertWorkerDeployment(firstAgent, 0, 1);
        assertWorkerDeployment(secondAgent, 0, 0);
        assertWorkerDeployment(thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_clientWorkerOverflow() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 0, 5, 0);

        assertWorkerDeployment(firstAgent, 0, 2);
        assertWorkerDeployment(secondAgent, 0, 2);
        assertWorkerDeployment(thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers1() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 2, 3, 1);

        assertWorkerDeployment(firstAgent, 2, 0);
        assertWorkerDeployment(secondAgent, 0, 2);
        assertWorkerDeployment(thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers2() {
        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 2, 3, 2);

        assertWorkerDeployment(firstAgent, 1, 0);
        assertWorkerDeployment(secondAgent, 1, 0);
        assertWorkerDeployment(thirdAgent, 0, 3);
    }

    @Test
    public void testGenerateFromArguments_withIncrementalDeployment_addFirstClientWorkerToLeastCrowdedAgent() {
        WorkerProcessSettings firstWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );
        WorkerProcessSettings secondWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );

        componentRegistry.addWorkers(firstAgent, singletonList(firstWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(secondWorker));

        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 0, 4, 0);

        assertWorkerDeployment(firstAgent, 0, 1);
        assertWorkerDeployment(secondAgent, 0, 1);
        assertWorkerDeployment(thirdAgent, 0, 2);
    }

    @Test
    public void testGenerateFromArguments_withIncrementalDeployment_withDedicatedMembers_addClientsToCorrectAgents() {
        WorkerProcessSettings firstWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );
        WorkerProcessSettings secondWorker = new WorkerProcessSettings(
                1,
                WorkerType.CLIENT,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );
        WorkerProcessSettings thirdWorker = new WorkerProcessSettings(
                2,
                WorkerType.CLIENT,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );

        componentRegistry.addWorkers(firstAgent, singletonList(firstWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(secondWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(thirdWorker));
        componentRegistry.addWorkers(thirdAgent, singletonList(secondWorker));
        componentRegistry.addWorkers(thirdAgent, singletonList(thirdWorker));

        workerDeployment = generateFromArguments(componentRegistry, workerParametersMap, 0, 3, 1);

        assertWorkerDeployment(firstAgent, 0, 0);
        assertWorkerDeployment(secondAgent, 0, 2);
        assertWorkerDeployment(thirdAgent, 0, 1);
    }

    private void assertWorkerDeployment(SimulatorAddress agentAddress, int memberCount, int clientCount) {
        List<WorkerProcessSettings> settingsList = workerDeployment.get(agentAddress);
        assertNotNull("Could not find WorkerProcessSettings at index " + agentAddress
                + ", workerDeployment: " + workerDeployment, settingsList);

        int actualMemberWorkerCount = 0;
        int actualClientWorkerCount = 0;
        for (WorkerProcessSettings workerProcessSettings : settingsList) {
            if (workerProcessSettings.getWorkerType().isMember()) {
                actualMemberWorkerCount++;
            } else {
                actualClientWorkerCount++;
            }
        }
        String prefix = String.format("Agent %s members: %d clients: %d",
                agentAddress,
                actualMemberWorkerCount,
                actualClientWorkerCount);

        assertEquals(prefix + " (memberWorkerCount)", memberCount, actualMemberWorkerCount);
        assertEquals(prefix + " (clientWorkerCount)", clientCount, actualClientWorkerCount);
    }
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
        ComponentRegistry componentRegistry = new ComponentRegistry();
        for (AgentData agent : agents) {
            componentRegistry.addAgent(agent.getPublicAddress(), agent.getPrivateAddress());
        }

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getVersionSpec()).thenReturn("outofthebox");

        Map<WorkerType, WorkerParameters> workerParametersMap = new HashMap<WorkerType,WorkerParameters>();
        workerParametersMap.put(WorkerType.MEMBER, workerParameters);

        DeploymentPlan deploymentPlan = DeploymentPlan.createDeploymentPlan(componentRegistry, workerParametersMap,
                memberCount, clientCount, 0);

        assertEquals(singleton("outofthebox"), deploymentPlan.getVersionSpecs());
    }
}
