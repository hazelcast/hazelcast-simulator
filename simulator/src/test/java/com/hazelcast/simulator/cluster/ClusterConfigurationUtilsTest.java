package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static com.hazelcast.simulator.cluster.ClusterConfigurationUtils.fromXml;
import static com.hazelcast.simulator.cluster.ClusterConfigurationUtils.toXml;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.worker.WorkerType.CLIENT;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterConfigurationUtilsTest {

    private static final String MEMBER_HZ_CONFIG_FILE = "dist/src/main/dist/conf/hazelcast.xml";
    private static final String CLIENT_HZ_CONFIG_FILE = "dist/src/main/dist/conf/client-hazelcast.xml";

    private static final Logger LOGGER = Logger.getLogger(ClusterConfigurationUtilsTest.class);

    private ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);

    @Before
    public void setUp() {
        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getHazelcastVersionSpec()).thenReturn("defaultHzVersion");
        when(workerParameters.getMemberHzConfig()).thenReturn("defaultMemberHzConfig");
        when(workerParameters.getMemberJvmOptions()).thenReturn("defaultMemberJvmOptions");

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        ComponentRegistry componentRegistry = new ComponentRegistry();

        WorkerConfigurationConverter converter = new WorkerConfigurationConverter(5701, "defaultLicenseKey", workerParameters,
                simulatorProperties, componentRegistry);

        when(clusterLayoutParameters.getWorkerConfigurationConverter()).thenReturn(converter);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ClusterConfigurationUtils.class);
    }

    @Test
    public void testToXml_fromXml() throws Exception {
        WorkerConfiguration memberWorkerConfiguration = new WorkerConfiguration("memberWorker", MEMBER, "hzVersion",
                MEMBER_HZ_CONFIG_FILE, "jvmOptions");
        WorkerConfiguration clientWorkerConfiguration = new WorkerConfiguration("clientWorker", CLIENT, "hzVersion",
                CLIENT_HZ_CONFIG_FILE, "jvmOptions");

        NodeConfiguration node1 = new NodeConfiguration();
        node1.addWorkerConfiguration("memberWorker", 1);
        node1.addWorkerConfiguration("clientWorker", 5);

        NodeConfiguration node2 = new NodeConfiguration();
        node2.addWorkerConfiguration("memberWorker", 1);
        node2.addWorkerConfiguration("clientWorker", 5);

        NodeConfiguration node3 = new NodeConfiguration();
        node3.addWorkerConfiguration("clientWorker", 10);

        ClusterConfiguration expectedClusterConfiguration = new ClusterConfiguration();

        expectedClusterConfiguration.addWorkerConfiguration(memberWorkerConfiguration);
        expectedClusterConfiguration.addWorkerConfiguration(clientWorkerConfiguration);

        expectedClusterConfiguration.addNodeConfiguration(node1);
        expectedClusterConfiguration.addNodeConfiguration(node2);
        expectedClusterConfiguration.addNodeConfiguration(node3);

        String xml = toXml(clusterLayoutParameters, expectedClusterConfiguration);
        LOGGER.info(xml);

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        ClusterConfiguration actualClusterConfiguration = fromXml(clusterLayoutParameters);

        assertClusterConfiguration(expectedClusterConfiguration, actualClusterConfiguration);
    }

    @Test
    public void testFromXml_withMissingAttributes() {
        String xml = format("<clusterConfiguration>%n"
                + "  <workerConfiguration name=\"withHzVersion\" type=\"MEMBER\" hzVersion=\"hzVersion\"/>%n"
                + "  <workerConfiguration name=\"withHzConfigFile\" type=\"MEMBER\" hzConfigFile=\"%s\"/>%n"
                + "  <workerConfiguration name=\"withHzConfig\" type=\"MEMBER\" hzConfig=\"hzConfig\"/>%n"
                + "  <workerConfiguration name=\"withJvmOptions\" type=\"MEMBER\" jvmOptions=\"jvmOptions\"/>%n"
                + "  <nodeConfiguration>%n"
                + "    <workerGroup configuration=\"withHzVersion\" count=\"1\"/>%n"
                + "  </nodeConfiguration>%n"
                + "</clusterConfiguration>",
                MEMBER_HZ_CONFIG_FILE);

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        ClusterConfiguration clusterConfiguration = fromXml(clusterLayoutParameters);

        WorkerConfiguration withHzVersion = clusterConfiguration.getWorkerConfiguration("withHzVersion");
        assertEquals(MEMBER, withHzVersion.getType());
        assertEquals("hzVersion", withHzVersion.getHzVersion());
        assertEquals("defaultMemberHzConfig", withHzVersion.getHzConfig());
        assertEquals("defaultMemberJvmOptions", withHzVersion.getJvmOptions());

        WorkerConfiguration withHzConfigFile = clusterConfiguration.getWorkerConfiguration("withHzConfigFile");
        assertEquals(MEMBER, withHzConfigFile.getType());
        assertEquals("defaultHzVersion", withHzConfigFile.getHzVersion());
        assertNotNull(withHzConfigFile.getHzConfig());
        assertTrue(withHzConfigFile.getHzConfig().contains("defaultLicenseKey"));
        assertEquals("defaultMemberJvmOptions", withHzConfigFile.getJvmOptions());

        WorkerConfiguration withHzConfig = clusterConfiguration.getWorkerConfiguration("withHzConfig");
        assertEquals(MEMBER, withHzConfig.getType());
        assertEquals("defaultHzVersion", withHzConfig.getHzVersion());
        assertEquals("hzConfig", withHzConfig.getHzConfig());
        assertEquals("defaultMemberJvmOptions", withHzConfig.getJvmOptions());

        WorkerConfiguration withJvmOptions = clusterConfiguration.getWorkerConfiguration("withJvmOptions");
        assertEquals(MEMBER, withJvmOptions.getType());
        assertEquals("defaultHzVersion", withJvmOptions.getHzVersion());
        assertEquals("defaultMemberHzConfig", withJvmOptions.getHzConfig());
        assertEquals("jvmOptions", withJvmOptions.getJvmOptions());
    }

    private static void assertClusterConfiguration(ClusterConfiguration expectedClusterConfiguration,
                                                   ClusterConfiguration actualClusterConfiguration) {
        assertEquals(expectedClusterConfiguration.size(), actualClusterConfiguration.size());
        Iterator<NodeConfiguration> nodeConfigurationIterator = actualClusterConfiguration.getNodeConfigurations().iterator();
        for (NodeConfiguration expectedNodeConfiguration : expectedClusterConfiguration.getNodeConfigurations()) {
            NodeConfiguration actualNodeConfiguration = nodeConfigurationIterator.next();
            assertNodeConfiguration(expectedNodeConfiguration, actualNodeConfiguration);
        }
    }

    private static void assertNodeConfiguration(NodeConfiguration expectedNodeConfiguration,
                                                NodeConfiguration actualNodeConfiguration) {
        Iterator<WorkerGroup> workerGroupIterator = actualNodeConfiguration.getWorkerGroups().iterator();
        for (WorkerGroup expectedWorkerGroup : expectedNodeConfiguration.getWorkerGroups()) {
            WorkerGroup actualWorkerGroup = workerGroupIterator.next();
            assertWorkerGroup(expectedWorkerGroup, actualWorkerGroup);
        }
    }

    private static void assertWorkerGroup(WorkerGroup expectedWorkerGroup, WorkerGroup actualWorkerGroup) {
        assertEquals(expectedWorkerGroup.getCount(), actualWorkerGroup.getCount());
        assertEquals(expectedWorkerGroup.getConfiguration(), actualWorkerGroup.getConfiguration());
    }
}
