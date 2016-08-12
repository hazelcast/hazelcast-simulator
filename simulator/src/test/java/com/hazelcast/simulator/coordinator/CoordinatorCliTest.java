package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTY_CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
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
    private static File propertiesFile;

    private final List<String> args = new ArrayList<String>();
    private String sessionId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setDistributionUserDir();
        createAgentsFileWithLocalhost();

        testSuiteFile = ensureExistingFile("test.properties");
        appendText("# CoordinatorCliTest", testSuiteFile);

        propertiesFile = ensureExistingFile("simulator.properties");
    }

    @Before
    public void before(){
        args.add("--sessionId");
        sessionId = "CoordinatorCliTest-" + System.currentTimeMillis();
        args.add(sessionId);
    }

    @AfterClass
    public static void afterClass() {
        resetUserDir();
        deleteAgentsFile();

        deleteQuiet(testSuiteFile);
        deleteQuiet(propertiesFile);
    }

    @After
    public void after() {
        deleteQuiet(new File(sessionId).getAbsoluteFile());
    }

    @Test
    public void testInit() {
        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_withCloudProviderStatic() {
        appendText(format("%s=%s%n", PROPERTY_CLOUD_PROVIDER, PROVIDER_STATIC), propertiesFile);

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(423, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(TimeUnit.SECONDS.toSeconds(3), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(TimeUnit.MINUTES.toSeconds(5), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(TimeUnit.HOURS.toSeconds(4), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(TimeUnit.DAYS.toSeconds(23), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withZero() {
        // we are fine with a zero time execution, since it's useful for a dry run
        args.add("--duration");
        args.add("0s");

        Coordinator coordinator = createCoordinator();
        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(0, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNegativeTime() {
        args.add("--duration");
        args.add("-1");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNumberFormatException() {
        args.add("--duration");
        args.add("numberFormatException");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_warmupDuration_withNumberFormatException() {
        args.add("--warmupDuration");
        args.add("numberFormatException");

        createCoordinator();
    }

    @Test
    public void testInit_warmupDuration() {
        args.add("--duration");
        args.add("10s");
        args.add("--warmupDuration");
        args.add("5s");

        Coordinator coordinator = createCoordinator();
        TestSuite testSuite = coordinator.getTestSuite();
        assertFalse(testSuite.isWaitForTestCase());
        assertEquals(10, testSuite.getDurationSeconds());
        assertEquals(5, testSuite.getWarmupDurationSeconds());
    }

    @Test
    public void testInit_warmupDuration_withZero() {
        args.add("--warmupDuration");
        args.add("0s");

        Coordinator coordinator = createCoordinator();
        TestSuite testSuite = coordinator.getTestSuite();
        assertEquals(0, testSuite.getWarmupDurationSeconds());
    }

    @Test
    public void testInit_waitForTestCaseCompletion() {
        args.add("--waitForTestCaseCompletion");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertTrue(testSuite.isWaitForTestCase());
        assertEquals(0, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_waitForTestCaseCompletion_and_duration() {
        args.add("--waitForTestCaseCompletion");
        args.add("--duration");
        args.add("42s");

        Coordinator coordinator = createCoordinator();

        TestSuite testSuite = coordinator.getTestSuite();
        assertTrue(testSuite.isWaitForTestCase());
        assertEquals(42, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_workerClassPath() {
        args.add("--workerClassPath");
        args.add("*.jar");

        Coordinator coordinator = createCoordinator();

        assertEquals("*.jar", coordinator.getCoordinatorParameters().getWorkerClassPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noWorkersDefined() {
        args.add("--memberWorkerCount");
        args.add("0");
        args.add("--clientWorkerCount");
        args.add("0");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_propertiesFile() {
        args.add("--propertiesFile");
        args.add("not.found");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_tooMany() {
        args.add("test.properties");
        args.add("test.properties");

        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_notFound() {
        args.add("not.found");

        createCoordinator();
    }

    @Test(expected = Exception.class)
    public void testInit_syncToTestPhase_invalid() {
        args.add("--syncToTestPhase");
        args.add("INVALID");

        createCoordinator();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.getLastTestPhase(), coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalPrepare() {
        args.add("--syncToTestPhase");
        args.add("GLOBAL_PREPARE");

        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.GLOBAL_PREPARE, coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");

        Coordinator coordinator = createCoordinator();

        assertEquals(TestPhase.LOCAL_VERIFY, coordinator.getCoordinatorParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_memberConfigFileInWorkDir() {
        File memberConfigFile = new File("hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, memberConfigFile);

        try {
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
            createCoordinator();
        } finally {
            deleteQuiet(clusterConfigFile);
        }
    }

    @Test
    public void testInit_withLocalSetup() {
        File simulatorProperties = new File("simulator.properties").getAbsoluteFile();
        writeText(format("%s=%s", PROPERTY_CLOUD_PROVIDER, CloudProviderUtils.PROVIDER_LOCAL), simulatorProperties);

        try {
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
        return new CoordinatorCli(getArgs()).getCoordinator();
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
