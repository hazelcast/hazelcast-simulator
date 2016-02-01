package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.simulator.cluster.AgentWorkerMode.CLIENT;
import static com.hazelcast.simulator.cluster.AgentWorkerMode.CUSTOM;
import static com.hazelcast.simulator.cluster.AgentWorkerMode.MEMBER;
import static com.hazelcast.simulator.cluster.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.cluster.ClusterUtils.initMemberLayout;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterUtilsTest {

    private final ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
    private final WorkerParameters workerParameters = mock(WorkerParameters.class);
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private List<AgentWorkerLayout> agentWorkerLayouts;

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        WorkerConfigurationConverter converter = new WorkerConfigurationConverter(5701, "defaultLicenseKey", workerParameters,
                simulatorProperties, componentRegistry);

        when(clusterLayoutParameters.getWorkerConfigurationConverter()).thenReturn(converter);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ClusterUtils.class);
    }

    @Test
    public void testFormatIpAddresses_sameAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "192.168.0.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = ClusterUtils.formatIpAddresses(agentWorkerLayout);
        assertTrue(ipAddresses.contains("192.168.0.1"));
    }

    @Test
    public void testFormatIpAddresses_differentAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "172.16.16.1");
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, MIXED);
        String ipAddresses = ClusterUtils.formatIpAddresses(agentWorkerLayout);
        assertTrue(ipAddresses.contains("192.168.0.1"));
        assertTrue(ipAddresses.contains("172.16.16.1"));
    }

    @Test
    public void testInitMemberLayout_fromXml() {
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

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, CUSTOM, 1, 0);
        assertAgentWorkerLayout(1, CUSTOM, 0, 2);
        assertAgentWorkerLayout(2, CUSTOM, 3, 4);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_fromXml_countMismatch() {
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

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_fromXml_hzConfigFileNotExists() {
        String xml = "<clusterConfiguration>"
                + NEW_LINE + "\t<workerConfiguration name=\"memberWorker\" type=\"MEMBER\" hzConfigFile=\"notExists\"/>"
                + NEW_LINE + "\t<nodeConfiguration>"
                + NEW_LINE + "\t<workerGroup configuration=\"memberWorker\" count=\"1\"/>"
                + NEW_LINE + "\t</nodeConfiguration>"
                + NEW_LINE + "</clusterConfiguration>";

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }

    @Test
    public void testInitMemberLayout_dedicatedMemberCountEqualsAgentCount() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(3);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(1);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MEMBER, 1, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, MEMBER, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_dedicatedMemberCountHigherThanAgentCount() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(5);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(1);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }

    @Test
    public void testInitMemberLayout_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(2);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(0);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(1);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MEMBER, 0, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(3);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(0);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(1);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_noWorkersDefined() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(0);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }

    @Test
    public void testInitMemberLayout_singleMemberWorker() {
        when(workerParameters.isMonitorPerformance()).thenReturn(true);
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(1);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MIXED, 1, 0);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testInitMemberLayout_memberWorkerOverflow() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(4);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MIXED, 2, 0);
        assertAgentWorkerLayout(1, MIXED, 1, 0);
        assertAgentWorkerLayout(2, MIXED, 1, 0);
    }

    @Test
    public void testInitMemberLayout_singleClientWorker() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(0);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(1);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MIXED, 0, 1);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testClientWorkerOverflow() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(0);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(5);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MIXED, 0, 2);
        assertAgentWorkerLayout(1, MIXED, 0, 2);
        assertAgentWorkerLayout(2, MIXED, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers1() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(1);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(2);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(3);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        assertAgentWorkerLayout(0, MEMBER, 2, 0);
        assertAgentWorkerLayout(1, CLIENT, 0, 2);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers2() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(2);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(2);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(3);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);
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
