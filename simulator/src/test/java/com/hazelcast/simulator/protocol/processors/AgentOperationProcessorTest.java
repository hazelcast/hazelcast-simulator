package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.CoordinatorLogger;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import com.hazelcast.simulator.worker.WorkerType;
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.common.JavaProfiler.FLIGHTRECORDER;
import static com.hazelcast.simulator.common.JavaProfiler.HPROF;
import static com.hazelcast.simulator.common.JavaProfiler.PERF;
import static com.hazelcast.simulator.common.JavaProfiler.VTUNE;
import static com.hazelcast.simulator.common.JavaProfiler.YOURKIT;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AgentOperationProcessorTest {

    private static final int DEFAULT_TEST_TIMEOUT = 30000;
    private static final int DEFAULT_STARTUP_TIMEOUT = 10;

    private final ExceptionLogger exceptionLogger = mock(ExceptionLogger.class);
    private final WorkerJvmFailureMonitor failureMonitor = mock(WorkerJvmFailureMonitor.class);
    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager();
    private final ExecutorService executorService = createFixedThreadPool(3, "AgentOperationProcessorTest");

    private TestSuite testSuite;
    private File testSuiteDir;

    private AgentOperationProcessor processor;

    @Before
    public void setUp() {
        setDistributionUserDir();

        File workersDir = new File(getSimulatorHome(), "workers");
        testSuite = new TestSuite("AgentOperationProcessorTest");
        testSuiteDir = new File(workersDir, testSuite.getId()).getAbsoluteFile();

        AgentConnector agentConnector = mock(AgentConnector.class);
        CoordinatorLogger coordinatorLogger = mock(CoordinatorLogger.class);

        Agent agent = mock(Agent.class);
        when(agent.getAddressIndex()).thenReturn(1);
        when(agent.getPublicAddress()).thenReturn("127.0.0.1");
        when(agent.getTestSuite()).thenReturn(testSuite);
        when(agent.getTestSuiteDir()).thenReturn(testSuiteDir);
        when(agent.getAgentConnector()).thenReturn(agentConnector);
        when(agent.getCoordinatorLogger()).thenReturn(coordinatorLogger);
        when(agent.getWorkerJvmFailureMonitor()).thenReturn(failureMonitor);

        processor = new AgentOperationProcessor(exceptionLogger, agent, workerJvmManager, executorService);
    }

    @After
    public void tearDown() throws Exception {
        resetUserDir();
        deleteLogs();

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new CreateTestOperation(1, new TestCase("AgentOperationProcessorTest"));
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_IntegrationTestOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void testInitTestSuiteOperation() throws Exception {
        SimulatorOperation operation = new InitTestSuiteOperation(testSuite);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);
        assertTrue(testSuiteDir.exists());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);
        assertWorkerLifecycle();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withStartupException() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(true, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withStartupTimeout() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, 0);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withUploadDirectory() throws Exception {
        File uploadDir = ensureExistingDirectory(testSuiteDir, "upload");
        ensureExistingFile(uploadDir, "testFile");

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);

        assertThatFileExistsInWorkerHomes("testFile");
        assertWorkerLifecycle();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withProfilerYourKit() throws Exception {
        String profilerSettings = "-agentpath:${SIMULATOR_HOME}/yourkit/linux-x86-64/libyjpagent.so=dir=${WORKER_HOME}"
                + ",sampling,snapshot_name_format=test";

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT, YOURKIT, profilerSettings);
        if (responseType == SUCCESS) {
            assertWorkerLifecycle();
            assertThatFileExistsInWorkerHomes("test-shutdown.snapshot");
        } else {
            System.err.println("YourKit is not running on this system!");
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withProfilerFlightRecorder() throws Exception {
        String profilerSettings = "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
                + " -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=test.jfr";

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT, FLIGHTRECORDER, profilerSettings);
        if (responseType == SUCCESS) {
            assertWorkerLifecycle();
            assertThatFileExistsInWorkerHomes("test.jfr");
        } else {
            System.err.println("Flight Recorder is not running on this system!");
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withProfilerHPROF() throws Exception {
        String profilerSettings = "-agentlib:hprof=cpu=samples,depth=10";

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT, HPROF, profilerSettings);
        if (responseType == SUCCESS) {
            assertWorkerLifecycle();
            assertThatFileExistsInWorkerHomes("java.hprof.txt");
        } else {
            System.err.println("HPROF is not running on this system!");
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withProfilerPerf() throws Exception {
        String profilerSettings = "perf record -o perf.data --quiet";

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT, PERF, profilerSettings);
        if (responseType == SUCCESS) {
            assertWorkerLifecycle();
            assertThatFileExistsInWorkerHomes("perf.data");
        } else {
            System.err.println("PERF is not running on this system!");
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testCreateWorkerOperation_withProfilerVTune() throws Exception {
        String profilerSettings = "/opt/intel/vtune_amplifier_xe/bin64/amplxe-cl -collect hotspots -report-output test.vtune";

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT, VTUNE, profilerSettings);
        if (responseType == SUCCESS) {
            assertWorkerLifecycle();
            assertThatFileExistsInWorkerHomes("test.vtune");
        } else {
            System.err.println("VTune is not running on this system!");
        }
    }

    @Test
    public void testStartTimeoutDetectionOperation() throws Exception {
        SimulatorOperation operation = new StartTimeoutDetectionOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        verify(failureMonitor).startTimeoutDetection();
    }

    @Test
    public void testStopTimeoutDetectionOperation() throws Exception {
        SimulatorOperation operation = new StopTimeoutDetectionOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        verify(failureMonitor).stopTimeoutDetection();
    }

    private ResponseType testCreateWorkerOperation(boolean withStartupException, int startupTimeout) throws Exception {
        return testCreateWorkerOperation(withStartupException, startupTimeout, JavaProfiler.NONE, null);
    }

    private ResponseType testCreateWorkerOperation(boolean withStartupException, int startupTimeout, JavaProfiler javaProfiler,
                                                   String profilerSettings) throws Exception {
        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerType()).thenReturn(WorkerType.INTEGRATION_TEST);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);
        when(workerJvmSettings.getHazelcastConfig()).thenReturn("");
        when(workerJvmSettings.getLog4jConfig()).thenReturn(fileAsText("dist/src/main/dist/conf/worker-log4j.xml"));
        when(workerJvmSettings.getProfiler()).thenReturn(javaProfiler);
        when(workerJvmSettings.getProfilerSettings()).thenReturn(profilerSettings);
        when(workerJvmSettings.getNumaCtl()).thenReturn(withStartupException ? null : "none");
        when(workerJvmSettings.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.BRING_MY_OWN);
        when(workerJvmSettings.getWorkerStartupTimeout()).thenReturn(startupTimeout);
        when(workerJvmSettings.getJvmOptions()).thenReturn("-verbose:gc");

        SimulatorOperation operation = new CreateWorkerOperation(singletonList(workerJvmSettings));
        return processor.processOperation(getOperationType(operation), operation, COORDINATOR);
    }

    private void assertWorkerLifecycle() throws InterruptedException {
        for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
            File workerDir = new File(testSuiteDir, workerJvm.getId());
            assertTrue(workerDir.exists());

            try {
                workerJvm.getProcess().exitValue();
                fail("Expected IllegalThreadStateException since process should still be alive!");
            } catch (IllegalThreadStateException e) {
                EmptyStatement.ignore(e);
            }

            File pidFile = new File(workerDir, "worker.pid");
            String pid = fileAsText(pidFile);
            execute("kill " + pid);

            workerJvm.getProcess().waitFor();
            workerJvm.getProcess().exitValue();

            deleteQuiet(pidFile);
        }
    }

    private void assertThatFileExistsInWorkerHomes(String fileName) {
        for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
            File workerHome = new File(testSuiteDir, workerJvm.getId()).getAbsoluteFile();
            assertTrue(format("WorkerHome %s should exist", workerHome), workerHome.exists());

            File file = new File(workerHome, fileName);
            assertTrue(format("File %s should exist in %s", fileName, workerHome), file.exists());
        }
    }
}
