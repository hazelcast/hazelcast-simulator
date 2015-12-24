package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerJvmFailureMonitorTest {

    private static final int DEFAULT_CHECK_INTERVAL = 30;
    private static final int DEFAULT_SLEEP_TIME = 100;

    private static int addressIndex;

    private AgentConnector agentConnector;
    private WorkerJvmManager workerJvmManager;

    private WorkerJvm workerJvm;
    private File workerHome;

    private WorkerJvmFailureMonitor workerJvmFailureMonitor;

    @Before
    public void setUp() {
        Response response = mock(Response.class);

        agentConnector = mock(AgentConnector.class);
        when(agentConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);

        Agent agent = mock(Agent.class);
        when(agent.getAgentConnector()).thenReturn(agentConnector);

        workerJvmManager = new WorkerJvmManager();

        workerJvm = addWorkerJvm(workerJvmManager, getWorkerAddress(), true);
        addWorkerJvm(workerJvmManager, getWorkerAddress(), false);

        workerHome = workerJvm.getWorkerHome();

        workerJvmFailureMonitor = new WorkerJvmFailureMonitor(agent, workerJvmManager, DEFAULT_CHECK_INTERVAL);
    }

    @After
    public void tearDown() {
        workerJvmFailureMonitor.shutdown();

        deleteQuiet(workerHome);
    }

    @Test
    public void testRun_shouldSendNoFailures() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldDetectException_withTestId() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "WorkerJvmFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);

        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withEmptyTestId() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);

        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withNullTestId() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "null", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);

        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withOomeFile() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        createFile(workerHome, "worker.oome");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        assertThatWorkerHasBeenRemoved(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withHprofFile() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        createFile(workerHome, "java_pid3140.hprof");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        assertThatWorkerHasBeenRemoved(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldNotDetectInactivityIfDetectionNotStarted() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        workerJvm.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldNotDetectInactivityAfterDetectionIsStopped() {
        workerJvmFailureMonitor.startTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerJvmFailureMonitor.stopTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerJvm.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldDetectInactivity() {
        workerJvmFailureMonitor.startTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerJvm.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSentAtLeast(agentConnector, 1);
        assertThatWorkerHasBeenRemovedAtLeast(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(0);

        WorkerJvm exitWorker = addWorkerJvm(workerJvmManager, getWorkerAddress(), true, process);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        assertThatWorkerHasBeenRemoved(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);
        assertTrue(exitWorker.isFinished());
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsNonZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(134);

        WorkerJvm exitWorker = addWorkerJvm(workerJvmManager, getWorkerAddress(), true, process);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatFailureOperationHasBeenSent(agentConnector, 1);
        assertThatWorkerHasBeenRemoved(agentConnector, 1);
        verifyNoMoreInteractions(agentConnector);
        assertFalse(exitWorker.isFinished());
    }

    @Test
    public void testExceptionExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerJvmFailureMonitor.ExceptionExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    @Test
    public void testHProfExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerJvmFailureMonitor.HProfExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    private static SimulatorAddress getWorkerAddress() {
        return new SimulatorAddress(WORKER, 1, ++addressIndex, 0);
    }

    private static WorkerJvm addWorkerJvm(WorkerJvmManager workerJvmManager, SimulatorAddress address, boolean createWorkerHome) {
        Process process = mock(Process.class);
        when(process.exitValue()).thenThrow(new IllegalThreadStateException("process is still running"));

        return addWorkerJvm(workerJvmManager, address, createWorkerHome, process);
    }

    private static WorkerJvm addWorkerJvm(WorkerJvmManager workerJvmManager, SimulatorAddress address, boolean createWorkerHome,
                                          Process process) {
        int addressIndex = address.getAddressIndex();
        File workerHome = new File("worker" + address.getAddressIndex());
        WorkerJvm workerJvm = new WorkerJvm(address, "WorkerJvmFailureMonitorTest" + addressIndex, workerHome);
        workerJvm.setProcess(process);

        workerJvmManager.add(address, workerJvm);

        if (createWorkerHome) {
            ensureExistingDirectory(workerHome);
        }

        return workerJvm;
    }

    private static File createExceptionFile(File workerHome, String testId, String cause) {
        String targetFileName = "1.exception";

        File tmpFile = createFile(workerHome, targetFileName + "tmp");
        File exceptionFile = new File(workerHome, targetFileName);

        appendText(testId + NEW_LINE + cause, tmpFile);
        rename(tmpFile, exceptionFile);

        return exceptionFile;
    }

    private static File createFile(File workerHome, String fileName) {
        File file = new File(workerHome, fileName);
        ensureExistingFile(file);

        return file;
    }

    private static void assertThatFailureOperationHasBeenSent(AgentConnector agentConnector, int times) {
        assertThatFailureOperationHasBeenSent(agentConnector, times(times));
    }

    private static void assertThatFailureOperationHasBeenSentAtLeast(AgentConnector agentConnector, int atLeast) {
        assertThatFailureOperationHasBeenSent(agentConnector, atLeast(atLeast));
    }

    private static void assertThatFailureOperationHasBeenSent(AgentConnector agentConnector, VerificationMode mode) {
        verify(agentConnector, mode).write(eq(COORDINATOR), any(FailureOperation.class));
    }

    private static void assertThatWorkerHasBeenRemoved(AgentConnector agentConnector, int times) {
        assertThatWorkerHasBeenRemoved(agentConnector, times(times));
    }

    private static void assertThatWorkerHasBeenRemovedAtLeast(AgentConnector agentConnector, int atLeast) {
        assertThatWorkerHasBeenRemoved(agentConnector, atLeast(atLeast));
    }

    private static void assertThatWorkerHasBeenRemoved(AgentConnector agentConnector, VerificationMode mode) {
        verify(agentConnector, mode).removeWorker(anyInt());
    }

    private static void assertThatExceptionFileDoesNotExist(File firstExceptionFile) {
        if (firstExceptionFile.exists()) {
            fail("Exception file should be deleted: " + firstExceptionFile);
        }
    }
}
