package com.hazelcast.simulator.coordinator.deployment;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.simulator.coordinator.deployment.AgentWorkerMode.CLIENT;
import static com.hazelcast.simulator.coordinator.deployment.AgentWorkerMode.CUSTOM;
import static com.hazelcast.simulator.coordinator.deployment.AgentWorkerMode.MEMBER;
import static com.hazelcast.simulator.coordinator.deployment.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentUtils.generateFromArguments;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentUtils.generateFromXml;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentUtilsTest {

    private final WorkerParameters workerParameters = mock(WorkerParameters.class);
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private List<AgentWorkerLayout> agentWorkerLayouts;
    private WorkerConfigurationConverter converter;

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        converter = new WorkerConfigurationConverter(5701, "defaultLicenseKey", workerParameters, simulatorProperties,
                componentRegistry);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(DeploymentUtils.class);
    }

    @Test
    public void testFormatIpAddresses_sameAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "192.168.0.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = DeploymentUtils.formatIpAddresses(agentWorkerLayout);
        assertTrue(ipAddresses.contains("192.168.0.1"));
    }

    @Test
    public void testFormatIpAddresses_differentAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "172.16.16.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = DeploymentUtils.formatIpAddresses(agentWorkerLayout);
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

        agentWorkerLayouts = generateFromXml(componentRegistry, workerParameters, converter, xml);
        assertAgentWorkerLayout(0, CUSTOM, 1, 0);
        assertAgentWorkerLayout(1, CUSTOM, 0, 2);
        assertAgentWorkerLayout(2, CUSTOM, 3, 4);
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

        generateFromXml(componentRegistry, workerParameters, converter, xml);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromXml_hzConfigFileNotExists() {
        String xml = "<clusterConfiguration>"
                + NEW_LINE + "\t<workerConfiguration name=\"memberWorker\" type=\"MEMBER\" hzConfigFile=\"notExists\"/>"
                + NEW_LINE + "\t<nodeConfiguration>"
                + NEW_LINE + "\t<workerGroup configuration=\"memberWorker\" count=\"1\"/>"
                + NEW_LINE + "\t</nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        generateFromXml(componentRegistry, workerParameters, converter, xml);
    }

    @Test
    public void testGenerateFromArguments_dedicatedMemberCountEqualsAgentCount() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 1, 0, 3);

        assertAgentWorkerLayout(0, MEMBER, 1, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, MEMBER, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noAgents() {
        generateFromArguments(new ComponentRegistry(), workerParameters, 0, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountNegative() {
        generateFromArguments(componentRegistry, workerParameters, 0, 0, -1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountHigherThanAgentCount() {
        generateFromArguments(componentRegistry, workerParameters, 1, 0, 5);
    }

    @Test
    public void testGenerateFromArguments_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 0, 1, 2);

        assertAgentWorkerLayout(0, MEMBER, 0, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        generateFromArguments(componentRegistry, workerParameters, 0, 1, 3);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noWorkersDefined() {
        generateFromArguments(componentRegistry, workerParameters, 0, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_singleMemberWorker() {
        when(workerParameters.isMonitorPerformance()).thenReturn(true);

        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 1, 0, 0);

        assertAgentWorkerLayout(0, MIXED, 1, 0);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_memberWorkerOverflow() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 4, 0, 0);

        assertAgentWorkerLayout(0, MIXED, 2, 0);
        assertAgentWorkerLayout(1, MIXED, 1, 0);
        assertAgentWorkerLayout(2, MIXED, 1, 0);
    }

    @Test
    public void testGenerateFromArguments_singleClientWorker() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 0, 1, 0);

        assertAgentWorkerLayout(0, MIXED, 0, 1);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_clientWorkerOverflow() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 0, 5, 0);

        assertAgentWorkerLayout(0, MIXED, 0, 2);
        assertAgentWorkerLayout(1, MIXED, 0, 2);
        assertAgentWorkerLayout(2, MIXED, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers1() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 2, 3, 1);

        assertAgentWorkerLayout(0, MEMBER, 2, 0);
        assertAgentWorkerLayout(1, CLIENT, 0, 2);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers2() {
        agentWorkerLayouts = generateFromArguments(componentRegistry, workerParameters, 2, 3, 2);

        assertAgentWorkerLayout(0, MEMBER, 1, 0);
        assertAgentWorkerLayout(1, MEMBER, 1, 0);
        assertAgentWorkerLayout(2, CLIENT, 0, 3);
    }

    private void assertAgentWorkerLayout(int index, AgentWorkerMode mode, int memberCount, int clientCount) {
        AgentWorkerLayout layout = agentWorkerLayouts.get(index);
        assertNotNull("Could not find AgentWorkerLayout at index " + index, layout);

        String prefix = String.format("Agent %s members: %d clients: %d mode: %s",
                layout.getPublicAddress(),
                layout.getCount(WorkerType.MEMBER),
                layout.getCount(WorkerType.CLIENT),
                layout.getAgentWorkerMode());

        assertEquals(prefix + " (agentWorkerMode)", mode, layout.getAgentWorkerMode());
        assertEquals(prefix + " (memberWorkerCount)", memberCount, layout.getCount(WorkerType.MEMBER));
        assertEquals(prefix + " (clientWorkerCount)", clientCount, layout.getCount(WorkerType.CLIENT));
    }
}
