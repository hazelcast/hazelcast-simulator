package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoordinatorCliTest {

    private static final String HAZELCAST_XML = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config"
            + NEW_LINE + "  http://www.hazelcast.com/schema/config/hazelcast-config-3.6.xsd\""
            + NEW_LINE + "  xmlns=\"http://www.hazelcast.com/schema/config\""
            + NEW_LINE + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" />";

    private static final String CLUSTER_XML
            = "<clusterConfiguration>"
            + NEW_LINE + "\t<workerConfiguration name=\"test\" type=\"MEMBER\"/>"
            + NEW_LINE + "\t<nodeConfiguration>"
            + NEW_LINE + "\t\t<workerGroup configuration=\"test\" count=\"1\"/>"
            + NEW_LINE + "\t</nodeConfiguration>"
            + NEW_LINE + "</clusterConfiguration>";

    private static File testSuiteFile;

    private final List<String> args = new ArrayList<String>();

    @BeforeClass
    public static void setUp() throws Exception {
        setDistributionUserDir();
        createAgentsFileWithLocalhost();

        testSuiteFile = new File("test.properties");
        appendText("# CoordinatorCliTest", testSuiteFile);
    }

    @AfterClass
    public static void tearDown() {
        resetUserDir();
        deleteAgentsFile();

        deleteQuiet(testSuiteFile);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noDuration_noWaitForTestCase() {
        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withZero() {
        args.add("--duration");
        args.add("0s");

        createCoordinator();
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(TimeUnit.SECONDS.toSeconds(3), coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(TimeUnit.MINUTES.toSeconds(5), coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(TimeUnit.HOURS.toSeconds(4), coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(TimeUnit.DAYS.toSeconds(23), coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(423, coordinator.getTestSuite().getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withException() {
        args.add("--duration");
        args.add("numberFormatException");

        Coordinator coordinator = createCoordinator();

        assertFalse(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(423, coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_waitForTestCaseCompletion() {
        args.add("--waitForTestCaseCompletion");

        Coordinator coordinator = createCoordinator();

        assertTrue(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(0, coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_waitForTestCaseCompletion_and_duration() {
        args.add("--waitForTestCaseCompletion");
        args.add("--duration");
        args.add("42");

        Coordinator coordinator = createCoordinator();

        assertTrue(coordinator.getTestSuite().isWaitForTestCase());
        assertEquals(42, coordinator.getTestSuite().getDurationSeconds());
    }

    @Test
    public void testInit_workerClassPath() {
        args.add("--waitForTestCaseCompletion");
        args.add("--workerClassPath");
        args.add("*.jar");

        Coordinator coordinator = createCoordinator();

        assertEquals("*.jar", coordinator.getCoordinatorParameters().getWorkerClassPath());
    }

    @Test
    public void testInit_dedicatedMemberMachines() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("1");

        Coordinator coordinator = createCoordinator();

        assertEquals(1, coordinator.getClusterLayoutParameters().getDedicatedMemberMachineCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_dedicatedMemberMachines_negativeValue() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("-1");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noWorkersDefined() {
        args.add("--waitForTestCaseCompletion");
        args.add("--memberWorkerCount");
        args.add("0");
        args.add("--clientWorkerCount");
        args.add("0");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_propertiesFile() {
        args.add("--waitForTestCaseCompletion");
        args.add("--propertiesFile");
        args.add("not.found");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_tooMany() {
        args.add("test2.properties");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_notFound() {
        args.add("--waitForTestCaseCompletion");
        args.add("not.found");

        CoordinatorCli.init(getArgs(false));
    }

    @Test
    public void testInit_testSuite_default() {
        args.add("--waitForTestCaseCompletion");

        CoordinatorCli.init(getArgs(false));
    }

    @Test(expected = Exception.class)
    public void testInit_syncToTestPhase_invalid() {
        args.add("--waitForTestCaseCompletion");
        args.add("--syncToTestPhase");
        args.add("INVALID");

        createCoordinator();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        args.add("--waitForTestCaseCompletion");

        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.SETUP, coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalWarmup() {
        args.add("--waitForTestCaseCompletion");
        args.add("--syncToTestPhase");
        args.add("GLOBAL_WARMUP");

        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.GLOBAL_WARMUP, coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--waitForTestCaseCompletion");
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");

        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.LOCAL_VERIFY, coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_git() {
        args.add("--waitForTestCaseCompletion");
        args.add("--git");
        args.add("sha123456");

        Coordinator coordinator = createCoordinator();

        assertEquals("git=sha123456", coordinator.getCoordinatorParameters().getSimulatorProperties().getHazelcastVersionSpec());
    }

    @Test
    public void testInit_memberConfigFileInWorkDir() {
        File memberConfigFile = new File("hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, memberConfigFile);

        try {
            args.add("--waitForTestCaseCompletion");

            Coordinator coordinator = createCoordinator();
            assertEquals(HAZELCAST_XML, coordinator.getWorkerParameters().getMemberHzConfig());
        } finally {
            deleteQuiet(memberConfigFile);
        }
    }

    @Test
    public void testInit_clientConfigFileInWorkDir() {
        File clientConfigFile = new File("client-hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, clientConfigFile);

        try {
            args.add("--waitForTestCaseCompletion");

            Coordinator coordinator = createCoordinator();
            assertEquals(HAZELCAST_XML, coordinator.getWorkerParameters().getClientHzConfig());
        } finally {
            deleteQuiet(clientConfigFile);
        }
    }

    @Test
    public void testInit_clusterConfigFileInWorkDir() {
        File clusterConfigFile = new File("cluster.xml").getAbsoluteFile();
        writeText(CLUSTER_XML, clusterConfigFile);

        try {
            args.add("--waitForTestCaseCompletion");

            Coordinator coordinator = createCoordinator();
            assertEquals(CLUSTER_XML, coordinator.getClusterLayoutParameters().getClusterConfiguration());
        } finally {
            deleteQuiet(clusterConfigFile);
        }
    }

    @Test
    public void testInit_withLocalSetup() {
        File simulatorProperties = new File("simulator.properties").getAbsoluteFile();
        writeText("CLOUD_PROVIDER=local", simulatorProperties);

        try {
            args.add("--waitForTestCaseCompletion");
            args.add("--propertiesFile");
            args.add(simulatorProperties.getAbsolutePath());

            Coordinator coordinator = createCoordinator();

            ComponentRegistry componentRegistry = coordinator.getComponentRegistry();
            assertEquals(1, componentRegistry.agentCount());

            AgentData firstAgent = componentRegistry.getFirstAgent();
            assertEquals("localhost", firstAgent.getPublicAddress());
            assertEquals("localhost", firstAgent.getPrivateAddress());
        } finally {
            deleteQuiet(simulatorProperties);
        }
    }

    private Coordinator createCoordinator() {
        return CoordinatorCli.init(getArgs(true));
    }

    private String[] getArgs(boolean addDefaults) {
        if (addDefaults) {
            args.add("test.properties");
        }

        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
