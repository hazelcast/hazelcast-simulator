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
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AgentOperationProcessorTest {

    private final ExceptionLogger exceptionLogger = mock(ExceptionLogger.class);
    private final WorkerJvmFailureMonitor failureMonitor = mock(WorkerJvmFailureMonitor.class);

    private TestSuite testSuite;
    private File testSuiteDir;

    private WorkerJvmManager workerJvmManager = new WorkerJvmManager();

    private AgentOperationProcessor processor;

    @Before
    public void setUp() {
        testSuite = new TestSuite("AgentOperationProcessorTest");
        testSuiteDir = new File("workers", testSuite.getId()).getAbsoluteFile();

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

    @Test
    public void testCreateWorkerOperation() throws Exception {
        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerType()).thenReturn(WorkerType.INTEGRATION_TEST);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);
        when(workerJvmSettings.getHazelcastConfig()).thenReturn("");
        when(workerJvmSettings.getLog4jConfig()).thenReturn(fileAsText("dist/src/main/dist/conf/worker-log4j.xml"));
        when(workerJvmSettings.getProfiler()).thenReturn(JavaProfiler.NONE);
        when(workerJvmSettings.getNumaCtl()).thenReturn("none");
        when(workerJvmSettings.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.BRING_MY_OWN);
        when(workerJvmSettings.getWorkerStartupTimeout()).thenReturn(60);

        SimulatorOperation operation = new CreateWorkerOperation(singletonList(workerJvmSettings));
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
            File workerDir = new File(testSuiteDir, workerJvm.getId());
            assertTrue(workerDir.exists());

            assertTrue(workerJvm.getProcess().isAlive());

            File pidFile = new File(workerDir, "worker.pid");
            String pid = fileAsText(pidFile);
            execute("kill " + pid);

            workerJvm.getProcess().waitFor(10, TimeUnit.SECONDS);
            assertFalse(workerJvm.getProcess().isAlive());
        }
    }

    @Test
    public void testInitTestSuiteOperation() throws Exception {
        SimulatorOperation operation = new InitTestSuiteOperation(testSuite);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);
        assertTrue(testSuiteDir.exists());
    }

    @Test
    public void testStopTimeoutDetectionOperation() throws Exception {
        SimulatorOperation operation = new StopTimeoutDetectionOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);

        verify(failureMonitor).stopTimeoutDetection();
    }
}
