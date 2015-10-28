package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import java.util.Iterator;

import static com.hazelcast.simulator.cluster.ClusterConfigurationUtils.fromXml;
import static com.hazelcast.simulator.cluster.ClusterConfigurationUtils.toXml;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterConfigurationUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ClusterConfigurationUtils.class);
    }

    @Test
    public void testToXml_fromXml() throws Exception {
        ClusterConfiguration expectedClusterConfiguration = new ClusterConfiguration();

        WorkerConfiguration memberWorkerConfiguration = new WorkerConfiguration("memberWorker", MEMBER, "hzVersion",
                "memberHzConfigFile", "jvmOptions");
        WorkerConfiguration clientWorkerConfiguration = new WorkerConfiguration("clientWorker", WorkerType.CLIENT, "hzVersion",
                "clientHzConfigFile", "jvmOptions");

        expectedClusterConfiguration.addWorkerConfiguration(memberWorkerConfiguration);
        expectedClusterConfiguration.addWorkerConfiguration(clientWorkerConfiguration);

        NodeConfiguration node1 = new NodeConfiguration();
        node1.addWorkerConfiguration("memberWorker", 1);
        node1.addWorkerConfiguration("clientWorker", 5);

        NodeConfiguration node2 = new NodeConfiguration();
        node2.addWorkerConfiguration("memberWorker", 1);
        node2.addWorkerConfiguration("clientWorker", 5);

        NodeConfiguration node3 = new NodeConfiguration();
        node3.addWorkerConfiguration("clientWorker", 10);

        expectedClusterConfiguration.addNodeConfiguration(node1);
        expectedClusterConfiguration.addNodeConfiguration(node2);
        expectedClusterConfiguration.addNodeConfiguration(node3);

        String xml = toXml(expectedClusterConfiguration);
        System.out.println(xml);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        ClusterConfiguration actualClusterConfiguration = fromXml(xml, workerParameters);

        assertClusterConfiguration(expectedClusterConfiguration, actualClusterConfiguration);
    }

    @Test
    public void testFromXml_withMissingAttributes() {
        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getHazelcastVersionSpec()).thenReturn("defaultHzVersion");
        when(workerParameters.getMemberHzConfig()).thenReturn("defaultMemberHzConfig");
        when(workerParameters.getMemberJvmOptions()).thenReturn("defaultMemberJvmOptions");

        String xml = "<clusterConfiguration>\n"
                + "  <workerConfiguration name=\"withHzVersion\" type=\"MEMBER\" hzVersion=\"hzVersion\"/>\n"
                + "  <workerConfiguration name=\"withHzConfigFile\" type=\"MEMBER\" hzConfigFile=\"hzConfigFile\"/>\n"
                + "  <workerConfiguration name=\"withHzConfig\" type=\"MEMBER\" hzConfig=\"hzConfig\"/>\n"
                + "  <workerConfiguration name=\"withJvmOptions\" type=\"MEMBER\" jvmOptions=\"jvmOptions\"/>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"withHzVersion\" count=\"1\"/>\n"
                + "  </nodeConfiguration>\n"
                + "</clusterConfiguration>";

        ClusterConfiguration clusterConfiguration = fromXml(xml, workerParameters);

        WorkerConfiguration withHzVersion = clusterConfiguration.getWorkerConfiguration("withHzVersion");
        assertEquals(MEMBER, withHzVersion.getType());
        assertEquals("hzVersion", withHzVersion.getHzVersion());
        assertEquals("defaultMemberHzConfig", withHzVersion.getHzConfig());
        assertEquals("defaultMemberJvmOptions", withHzVersion.getJvmOptions());

        WorkerConfiguration withHzConfigFile = clusterConfiguration.getWorkerConfiguration("withHzConfigFile");
        assertEquals(MEMBER, withHzConfigFile.getType());
        assertEquals("defaultHzVersion", withHzConfigFile.getHzVersion());
        assertEquals("content of file hzConfigFile", withHzConfigFile.getHzConfig());
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
