package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTY_CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoordinatorCliTest {

    private static final String HAZELCAST_XML = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config"
            + NEW_LINE + "  http://www.hazelcast.com/schema/config/hazelcast-config-3.6.xsd\""
            + NEW_LINE + "  xmlns=\"http://www.hazelcast.com/schema/config\""
            + NEW_LINE + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" />";

    private static File testSuiteFile;
    private static File propertiesFile;

    private final List<String> args = new ArrayList<String>();
    private String sessionId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();

        testSuiteFile = ensureExistingFile("test.properties");

        appendText("# CoordinatorCliTest", testSuiteFile);

        propertiesFile = ensureExistingFile("simulator.properties");
    }

    @Before
    public void before() {
        args.add("--sessionId");
        sessionId = "CoordinatorCliTest-" + currentTimeMillis();
        args.add(sessionId);
    }

    @AfterClass
    public static void afterClass() {
        tearDownFakeEnvironment();
        deleteQuiet(testSuiteFile);
        deleteQuiet(propertiesFile);
    }

    @After
    public void after() {
        deleteQuiet(new File(sessionId).getAbsoluteFile());
    }

    @Test
    public void testInit() {
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_withCloudProviderStatic() {
        appendText(format("%s=%s%n", PROPERTY_CLOUD_PROVIDER, PROVIDER_STATIC), propertiesFile);

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testNoRemotePort() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText("COORDINATOR_PORT=0",simulatorProperties);

        args.add("--remote");

        createCoordinatorCli();
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(423, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.SECONDS.toSeconds(3), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.MINUTES.toSeconds(5), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.HOURS.toSeconds(4), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.DAYS.toSeconds(23), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withZero() {
        // we are fine with a zero time execution, since it's useful for a dry run
        args.add("--duration");
        args.add("0s");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(0, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNegativeTime() {
        args.add("--duration");
        args.add("-1");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNumberFormatException() {
        args.add("--duration");
        args.add("numberFormatException");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_warmup_withNumberFormatException() {
        args.add("--warmup");
        args.add("numberFormatException");

        createCoordinatorCli();
    }

    @Test
    public void testInit_warmup() {
        args.add("--duration");
        args.add("10s");
        args.add("--warmup");
        args.add("5s");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(10, testSuite.getDurationSeconds());
        assertEquals(5, testSuite.getWarmupSeconds());
    }

    @Test
    public void testInit_warmup_withZero() {
        args.add("--warmup");
        args.add("0s");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(0, testSuite.getWarmupSeconds());
    }

    @Test
    public void testInit_waitForDuration() {
        args.add("--duration");
        args.add("42s");

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(42, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_workerClassPath() {
        args.add("--workerClassPath");
        args.add("*.jar");

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals("*.jar", cli.coordinatorParameters.getWorkerClassPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noWorkersDefined() {
        args.add("--members");
        args.add("0");
        args.add("--clients");
        args.add("0");

        createCoordinatorCli();
    }

    @Test
    public void testInit_workersAndClients() {
        args.add("--members");
        args.add("2");
        args.add("--clients");
        args.add("1");

        CoordinatorCli cli = createCoordinatorCli();
        assertEquals(2, count(cli.deploymentPlan, WorkerType.MEMBER));
        assertEquals(1, count(cli.deploymentPlan, WorkerType.JAVA_CLIENT));
    }

    @Test
    public void testInit_workersAndClients_oldProperties() {
        args.add("--memberWorkerCount");
        args.add("2");
        args.add("--clientWorkerCount");
        args.add("1");

        CoordinatorCli cli = createCoordinatorCli();
        assertEquals(2, count(cli.deploymentPlan, WorkerType.MEMBER));
        assertEquals(1, count(cli.deploymentPlan, WorkerType.JAVA_CLIENT));
    }

    int count(DeploymentPlan deploymentPlan, WorkerType type) {
        Map<SimulatorAddress, List<WorkerProcessSettings>> deployment = deploymentPlan.getWorkerDeployment();
        int result = 0;
        for (List<WorkerProcessSettings> list : deployment.values()) {
            for(WorkerProcessSettings settings: list) {
                if (settings.getWorkerType().equals(type)) {
                    result++;
                }
            }
        }
        return result;
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_propertiesFile() {
        args.add("--propertiesFile");
        args.add("not.found");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_tooMany() {
        args.add("test.properties");
        args.add("test.properties");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_notFound() {
        args.add("not.found");

        createCoordinatorCli();
    }

    @Test(expected = Exception.class)
    public void testInit_syncToTestPhase_invalid() {
        args.add("--syncToTestPhase");
        args.add("INVALID");

        createCoordinatorCli();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.getLastTestPhase(), cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalPrepare() {
        args.add("--syncToTestPhase");
        args.add("GLOBAL_PREPARE");

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.GLOBAL_PREPARE, cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.LOCAL_VERIFY, cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_memberConfigFileInWorkDir() {
        File memberConfigFile = new File("hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, memberConfigFile);

        try {
            CoordinatorCli cli = createCoordinatorCli();
            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.MEMBER).getEnvironment().get("HAZELCAST_CONFIG"));
        } finally {
            deleteQuiet(memberConfigFile);
        }
    }

    @Test
    public void testInit_clientConfigFileInWorkDir() {
        File clientConfigFile = new File("client-hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, clientConfigFile);

        try {
            CoordinatorCli cli = createCoordinatorCli();
            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.JAVA_CLIENT).getEnvironment().get("HAZELCAST_CONFIG"));
        } finally {
            deleteQuiet(clientConfigFile);
        }
    }

    @Test
    public void testInit_withLocalSetup() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText(format("%s=%s", PROPERTY_CLOUD_PROVIDER, CloudProviderUtils.PROVIDER_LOCAL), simulatorProperties);

        try {
            CoordinatorCli cli = createCoordinatorCli();

            ComponentRegistry componentRegistry = cli.componentRegistry;
            assertEquals(1, componentRegistry.agentCount());

            AgentData firstAgent = componentRegistry.getFirstAgent();
            assertEquals("localhost", firstAgent.getPublicAddress());
            assertEquals("localhost", firstAgent.getPrivateAddress());
        } finally {
            deleteQuiet(simulatorProperties);
        }
    }

    private CoordinatorCli createCoordinatorCli() {
        return new CoordinatorCli(getArgs());
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
