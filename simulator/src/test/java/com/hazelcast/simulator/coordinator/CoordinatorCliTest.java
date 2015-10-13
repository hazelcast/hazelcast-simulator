package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoordinatorCliTest {

    private static String userDir;
    private static File agentsFile;
    private static File testSuiteFile;

    private final List<String> args = new ArrayList<String>();

    @BeforeClass
    public static void setUp() throws Exception {
        userDir = System.getProperty("user.dir");
        System.setProperty("user.dir", "./dist/src/main/dist");

        agentsFile = new File("agents.txt");
        appendText("127.0.0.1", agentsFile);

        testSuiteFile = new File("test.properties");
        appendText("# CoordinatorCliTest", testSuiteFile);
    }

    @AfterClass
    public static void tearDown() {
        System.setProperty("user.dir", userDir);

        deleteQuiet(agentsFile);
        deleteQuiet(testSuiteFile);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noDuration_noWaitForTestCase() {
        createCoordinator();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_noParameter() {
        args.add("--duration");
        args.add("--parallel");

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

    @Test(expected = CommandLineExitException.class)
    public void testInit_dedicatedMemberMachines_negativeValue() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("-1");

        createCoordinator();
    }

    @Test
    public void testInit_dedicatedMemberMachines() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("2");

        Coordinator coordinator = createCoordinator();

        assertEquals(2, coordinator.getClusterLayoutParameters().getDedicatedMemberMachineCount());
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
