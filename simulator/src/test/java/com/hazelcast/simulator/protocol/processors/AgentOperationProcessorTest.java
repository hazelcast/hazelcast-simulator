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
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
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
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AgentOperationProcessorTest {

    private static final int DEFAULT_STARTUP_TIMEOUT = 10;

    private final ExceptionLogger exceptionLogger = mock(ExceptionLogger.class);
    private final WorkerJvmFailureMonitor failureMonitor = mock(WorkerJvmFailureMonitor.class);
    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager();

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

        processor = new AgentOperationProcessor(exceptionLogger, agent, workerJvmManager);
    }

    @After
    public void tearDown() {
        resetUserDir();
        deleteLogs();
    }

    @Test
    public void testProcessOperation_UnsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.TEST_DATA);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void testShutdown_withInterruptedException() throws Exception {
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        AgentOperationProcessor processor = new AgentOperationProcessor(exceptionLogger, null, null, executorService) {
            @Override
            protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                                    SimulatorAddress sourceAddress) throws Exception {
                return null;
            }
        };

        processor.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(executorService);
    }

    @Test(timeout = 10000)
    public void testCreateWorkerOperation() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);
        assertWorkerLifecycle();
    }

    @Test
    public void testCreateWorkerOperation_withStartupException() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(true, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void testCreateWorkerOperation_withStartupTimeout() throws Exception {
        ResponseType responseType = testCreateWorkerOperation(false, 0);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test(timeout = 10000)
    public void testCreateWorkerOperation_withUploadDirectory() throws Exception {
        File uploadDir = new File(testSuiteDir, "upload");
        ensureExistingDirectory(uploadDir);

        File uploadFile = new File(uploadDir, "testFile");
        ensureExistingFile(uploadFile);

        ResponseType responseType = testCreateWorkerOperation(false, DEFAULT_STARTUP_TIMEOUT);
        assertEquals(SUCCESS, responseType);

        for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
            File workerDir = new File(testSuiteDir, workerJvm.getId());
            assertTrue(workerDir.exists());

            File uploadCopy = new File(workerDir, "testFile");
            assertTrue(uploadCopy.exists());
        }

        assertWorkerLifecycle();
    }

    @Test
    public void testInitTestSuiteOperation() throws Exception {
        SimulatorOperation operation = new InitTestSuiteOperation(testSuite);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        System.out.println(getSimulatorHome());
        System.out.println(testSuiteDir.getAbsolutePath());

        assertEquals(SUCCESS, responseType);
        assertTrue(testSuiteDir.exists());
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
        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerType()).thenReturn(WorkerType.INTEGRATION_TEST);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);
        when(workerJvmSettings.getHazelcastConfig()).thenReturn("");
        when(workerJvmSettings.getLog4jConfig()).thenReturn(fileAsText("dist/src/main/dist/conf/worker-log4j.xml"));
        when(workerJvmSettings.getProfiler()).thenReturn(JavaProfiler.NONE);
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
}
