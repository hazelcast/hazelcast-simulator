package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.agent.FailureSender;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.common.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class WorkerProcessFailureMonitorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private static final int DEFAULT_LAST_SEEN_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_CHECK_INTERVAL = 30;
    private static final int DEFAULT_SLEEP_TIME = 100;

    private static int addressIndex;

    private FailureSender failureSender;
    private WorkerProcessManager workerProcessManager;

    private WorkerProcess workerProcess;
    private File workerHome;

    private WorkerProcessFailureMonitor workerProcessFailureMonitor;

    @Before
    public void setUp() {
        failureSender = mock(FailureSender.class);
        when(failureSender.sendFailureOperation(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(true);

        workerProcessManager = new WorkerProcessManager();

        workerProcess = addWorkerJvm(workerProcessManager, getWorkerAddress(), true);
        addWorkerJvm(workerProcessManager, getWorkerAddress(), false);

        workerHome = workerProcess.getWorkerHome();

        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureSender, workerProcessManager,
                DEFAULT_LAST_SEEN_TIMEOUT_SECONDS, DEFAULT_CHECK_INTERVAL);
        workerProcessFailureMonitor.start();
    }

    @After
    public void tearDown() {
        workerProcessFailureMonitor.shutdown();

        for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
            deleteQuiet(workerProcess.getWorkerHome());
        }
        deleteQuiet("worker37");
        deleteQuiet("worker3");
        deleteQuiet("worker6");
        deleteQuiet("1.exception.sendFailure");
    }

    @Test
    public void testConstructor() {
        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureSender, workerProcessManager,
                DEFAULT_LAST_SEEN_TIMEOUT_SECONDS);
        workerProcessFailureMonitor.start();

        verifyZeroInteractions(failureSender);
    }

    @Test
    public void testRun_shouldSendNoFailures() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureSender);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testRun_shouldContinueAfterExceptionDuringDetection() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenThrow(new IllegalArgumentException("expected exception")).thenReturn(0);
        WorkerProcess exceptionWorker = addWorkerJvm(workerProcessManager, getWorkerAddress(), true, process);

        do {
            sleepMillis(DEFAULT_SLEEP_TIME);
        } while (!exceptionWorker.isFinished());

        assertFailureType(failureSender, WORKER_FINISHED);
    }

    @Test
    public void testRun_shouldContinueAfterErrorResponse() {
        Response failOnceResponse = mock(Response.class);
        when(failOnceResponse.getFirstErrorResponseType()).thenReturn(FAILURE_COORDINATOR_NOT_FOUND).thenReturn(SUCCESS);
        when(failureSender.sendFailureOperation(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false);

        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_TIMEOUT);
    }

    @Test
    public void testRun_shouldContinueAfterSendFailure() {
        when(failureSender.sendFailureOperation(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false)
                .thenReturn(true);

        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_TIMEOUT);
    }

    @Test
    public void testRun_shouldDetectException_withTestId() {
        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withEmptyTestId() {
        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withNullTestId() {
        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "null", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_shouldRenameFileIfFailureOperationCouldNotBeSent_withSingleErrorResponse() {
        when(failureSender.sendFailureOperation(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false)
                .thenReturn(true);

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
        assertThatRenamedExceptionFileExists(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_shouldRenameFileIfFailureOperationCouldNotBeSent_withContinuousErrorResponse() {
        when(failureSender.sendFailureOperation(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false);

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerHome, "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
        assertThatRenamedExceptionFileExists(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withOomeFile() {
        ensureExistingFile(workerHome, "worker.oome");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureType(failureSender, WORKER_OOM);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withHprofFile() {
        ensureExistingFile(workerHome, "java_pid3140.hprof");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureType(failureSender, WORKER_OOM);
    }

    @Test
    public void testRun_shouldDetectInactivity() {
        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureSender, WORKER_TIMEOUT);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionDisabled() {
        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureSender, workerProcessManager, -1,
                DEFAULT_CHECK_INTERVAL);
        workerProcessFailureMonitor.start();

        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerProcessFailureMonitor.stopTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureSender);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionNotStarted() {
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureSender);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_afterDetectionIsStopped() {
        workerProcessFailureMonitor.startTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerProcessFailureMonitor.stopTimeoutDetection();
        workerProcess.setLastSeen(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureSender);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testRun_shouldDetectWorkerFinished_whenExitValueIsZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(0);
        WorkerProcess exitWorker = addWorkerJvm(workerProcessManager, getWorkerAddress(), true, process);

        do {
            sleepMillis(DEFAULT_SLEEP_TIME);
        } while (!exitWorker.isFinished());

        assertTrue(exitWorker.isFinished());
        assertFailureType(failureSender, WORKER_FINISHED);
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsNonZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(134);
        WorkerProcess exitWorker = addWorkerJvm(workerProcessManager, getWorkerAddress(), true, process);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFalse(exitWorker.isFinished());
        assertFailureType(failureSender, WORKER_EXIT);
    }

    @Test
    public void testExceptionExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerProcessFailureMonitor.ExceptionExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    @Test
    public void testHProfExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerProcessFailureMonitor.HProfExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    private static SimulatorAddress getWorkerAddress() {
        return new SimulatorAddress(WORKER, 1, ++addressIndex, 0);
    }

    private static WorkerProcess addWorkerJvm(WorkerProcessManager workerProcessManager, SimulatorAddress address,
                                              boolean createWorkerHome) {
        Process process = mock(Process.class);
        when(process.exitValue()).thenThrow(new IllegalThreadStateException("process is still running"));

        return addWorkerJvm(workerProcessManager, address, createWorkerHome, process);
    }

    private static WorkerProcess addWorkerJvm(WorkerProcessManager workerProcessManager, SimulatorAddress address,
                                              boolean createWorkerHome,
                                              Process process) {
        int addressIndex = address.getAddressIndex();
        File workerHome = new File("worker" + address.getAddressIndex());
        WorkerProcess workerProcess = new WorkerProcess(address, "WorkerProcessFailureMonitorTest" + addressIndex, workerHome);
        workerProcess.setProcess(process);

        workerProcessManager.add(address, workerProcess);

        if (createWorkerHome) {
            ensureExistingDirectory(workerHome);
        }

        return workerProcess;
    }

    private static File createExceptionFile(File workerHome, String testId, String cause) {
        String targetFileName = "1.exception";

        File tmpFile = ensureExistingFile(workerHome, targetFileName + "tmp");
        File exceptionFile = new File(workerHome, targetFileName);

        appendText(testId + NEW_LINE + cause, tmpFile);
        rename(tmpFile, exceptionFile);

        return exceptionFile;
    }

    private void assertFailureType(FailureSender failureSender, FailureType failureType) {
        assertFailureTypeAtLeastOnce(failureSender, failureType, times(1));
    }

    private void assertFailureTypeAtLeastOnce(FailureSender failureSender, FailureType failureType) {
        assertFailureTypeAtLeastOnce(failureSender, failureType, atLeastOnce());
    }

    private void assertFailureTypeAtLeastOnce(FailureSender failureSender, FailureType failureType, VerificationMode mode) {
        verify(failureSender, mode).sendFailureOperation(anyString(),
                eq(failureType),
                any(WorkerProcess.class),
                any(String.class),
                any(String.class));
        verifyNoMoreInteractions(failureSender);
    }

    private static void assertThatExceptionFileDoesNotExist(File firstExceptionFile) {
        assertFalse("Exception file should be deleted: " + firstExceptionFile, firstExceptionFile.exists());
    }

    private static void assertThatRenamedExceptionFileExists(File exceptionFile) {
        File expectedFile = new File(exceptionFile.getName() + ".sendFailure");
        assertTrue("Exception file should be renamed: " + expectedFile.getName(), expectedFile.exists());
    }
}
