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

    private List<String> args = new ArrayList<String>();

    private Coordinator coordinator;

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
        coordinatorCliInit();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_noParameter() {
        args.add("--duration");
        args.add("--parallel");

        coordinatorCliInit();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withZero() {
        args.add("--duration");
        args.add("0s");

        coordinatorCliInit();
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(TimeUnit.SECONDS.toSeconds(3), coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(TimeUnit.MINUTES.toSeconds(5), coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(TimeUnit.HOURS.toSeconds(4), coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(TimeUnit.DAYS.toSeconds(23), coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(423, coordinator.getTestSuite().durationSeconds);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withException() {
        args.add("--duration");
        args.add("numberFormatException");

        coordinatorCliInit();

        assertFalse(coordinator.getTestSuite().waitForTestCase);
        assertEquals(423, coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_waitForTestCaseCompletion() {
        args.add("--waitForTestCaseCompletion");

        coordinatorCliInit();

        assertTrue(coordinator.getTestSuite().waitForTestCase);
        assertEquals(0, coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_waitForTestCaseCompletion_and_duration() {
        args.add("--waitForTestCaseCompletion");
        args.add("--duration");
        args.add("42");

        coordinatorCliInit();

        assertTrue(coordinator.getTestSuite().waitForTestCase);
        assertEquals(42, coordinator.getTestSuite().durationSeconds);
    }

    @Test
    public void testInit_workerClassPath() {
        args.add("--waitForTestCaseCompletion");
        args.add("--workerClassPath");
        args.add("*.jar");

        coordinatorCliInit();

        assertEquals("*.jar", coordinator.getParameters().getWorkerClassPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_dedicatedMemberMachines_negativeValue() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("-1");

        coordinatorCliInit();
    }

    @Test
    public void testInit_dedicatedMemberMachines() {
        args.add("--waitForTestCaseCompletion");
        args.add("--dedicatedMemberMachines");
        args.add("2");

        coordinatorCliInit();

        assertEquals(2, coordinator.getParameters().getDedicatedMemberMachineCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_propertiesFile() {
        args.add("--waitForTestCaseCompletion");
        args.add("--propertiesFile");
        args.add("not.found");

        coordinatorCliInit();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_tooMany() {
        args.add("test2.properties");

        coordinatorCliInit();
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

        coordinatorCliInit();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        args.add("--waitForTestCaseCompletion");

        coordinatorCliInit();

        assertEquals(TestPhase.SETUP, coordinator.getParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalWarmup() {
        args.add("--waitForTestCaseCompletion");
        args.add("--syncToTestPhase");
        args.add("GLOBAL_WARMUP");

        coordinatorCliInit();

        assertEquals(TestPhase.GLOBAL_WARMUP, coordinator.getParameters().getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--waitForTestCaseCompletion");
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");

        coordinatorCliInit();

        assertEquals(TestPhase.LOCAL_VERIFY, coordinator.getParameters().getLastTestPhaseToSync());
    }

    private void coordinatorCliInit() {
        coordinator = CoordinatorCli.init(getArgs(true));
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
