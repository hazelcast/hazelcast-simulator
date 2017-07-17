package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class CoordinatorCliTest {

    private static File testSuiteFile;
    private static File propertiesFile;

    private final List<String> args = new ArrayList<String>();
    private String sessionId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();

        testSuiteFile = ensureExistingFile("foobar_test.properties");

        appendText("# CoordinatorCliTest", testSuiteFile);
        appendText("\nclass=Foobar", testSuiteFile);

        propertiesFile = ensureExistingFile("simulator.properties");
    }

    @Before
    public void before() {
        sessionId = "CoordinatorCliTest-" + currentTimeMillis();
        args.add("--sessionId");
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
        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test(expected = BindException.class)
    public void test_whenEmptyTestSuiteFile() throws IOException {
        File file = File.createTempFile("foo", "bar");
        args.add(file.getAbsolutePath());
        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void test_whenNonExistingTestFile() throws IOException {
        File file = new File("not.exist.properties");
        args.add(file.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(1, testSuite.getTestCaseList().size());
    }

    @Test
    public void test_whenDirectString() throws IOException {
        String test = "class=Foo";
        args.add(test);
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(1, testSuite.getTestCaseList().size());
    }

    @Test(expected = BindException.class)
    public void test_whenDirectStringButEmpty() throws IOException {
        String test = "";
        args.add(test);
        createCoordinatorCli();
    }

    @Test
    public void testInit_withCloudProviderStatic() {
        appendText(format("%s=%s%n", CLOUD_PROVIDER, PROVIDER_STATIC), propertiesFile);

        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testWhenNoTestThenCoordinatorPortEnabled() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText("COORDINATOR_PORT=0", simulatorProperties);

        createCoordinatorCli();
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(423, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.SECONDS.toSeconds(3), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(MINUTES.toSeconds(5), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(HOURS.toSeconds(4), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(DAYS.toSeconds(23), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withZero() {
        // we are fine with a zero time execution, since it's useful for a dry run
        args.add("--duration");
        args.add("0s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(0, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNegativeTime() {
        args.add("--duration");
        args.add("-1");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNumberFormatException() {
        args.add("--duration");
        args.add("numberFormatException");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_warmup_withNumberFormatException() {
        args.add("--warmup");
        args.add("numberFormatException");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test
    public void testInit_waitForDuration() {
        args.add("--duration");
        args.add("42s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(42, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noWorkersDefined() {
        args.add("--members");
        args.add("0");
        args.add("--clients");
        args.add("0");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_negativeClients() {
        args.add("--members");
        args.add("1");
        args.add("--clients");
        args.add("-1");
        args.add(testSuiteFile.getAbsolutePath());
        createCoordinatorCli();
    }

    @Test
    public void testInit_workersAndClients() {
        args.add("--members");
        args.add("2");
        args.add("--clients");
        args.add("1");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();
        assertEquals(2, count(cli.deploymentPlan, "member"));
        assertEquals(1, count(cli.deploymentPlan, "javaclient"));
    }

    private int count(DeploymentPlan deploymentPlan, String workerType) {
        Map<SimulatorAddress, List<WorkerParameters>> deployment = deploymentPlan.getWorkerDeployment();
        int result = 0;
        for (List<WorkerParameters> list : deployment.values()) {
            for (WorkerParameters settings : list) {
                if (settings.getWorkerType().equals(workerType)) {
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
        args.add("not_found.properties");

        createCoordinatorCli();
    }

    @Test(expected = Exception.class)
    public void testInit_syncToTestPhase_invalid() {
        args.add("--syncToTestPhase");
        args.add("INVALID");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.getLastTestPhase(), cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalPrepare() {
        args.add("--syncToTestPhase");
        args.add("GLOBAL_PREPARE");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.GLOBAL_PREPARE, cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.LOCAL_VERIFY, cli.coordinatorParameters.getLastTestPhaseToSync());
    }


    @Test
    public void testInit_withLocalSetup() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText(format("%s=%s", CLOUD_PROVIDER, CloudProviderUtils.PROVIDER_LOCAL), simulatorProperties);
        writeText("COORDINATOR_PORT=5000\n", simulatorProperties);
        try {
            CoordinatorCli cli = createCoordinatorCli();

            Registry registry = cli.registry;
            assertEquals(1, registry.agentCount());

            AgentData firstAgent = registry.getFirstAgent();
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
