/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.FailureType.WORKER_ABNORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOME;
import static com.hazelcast.simulator.common.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
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

    private WorkerProcessFailureHandler failureHandler;
    private WorkerProcessManager workerProcessManager;


    private WorkerProcessFailureMonitor workerProcessFailureMonitor;
    private File workersHome;

    @Before
    public void before() {
        File simulatorHome = setupFakeEnvironment();
        workersHome = new File(simulatorHome, "workers");

        failureHandler = mock(WorkerProcessFailureHandler.class);
        when(failureHandler.handle(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(true);

        workerProcessManager = new WorkerProcessManager();

        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(
                failureHandler,
                workerProcessManager,
                DEFAULT_LAST_SEEN_TIMEOUT_SECONDS,
                DEFAULT_CHECK_INTERVAL);
        workerProcessFailureMonitor.start();
    }

    @After
    public void after() {
        workerProcessFailureMonitor.shutdown();

        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor() {
        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureHandler, workerProcessManager,
                DEFAULT_LAST_SEEN_TIMEOUT_SECONDS);
        workerProcessFailureMonitor.start();

        verifyZeroInteractions(failureHandler);
    }

    @Test
    public void testRun_shouldSendNoFailures() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureHandler);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testRun_shouldContinueAfterExceptionDuringDetection() {
        WorkerProcess workerProcess = addRunningWorkerProcess();
        Process process = workerProcess.getProcess();
        reset(process);
        when(process.exitValue()).thenThrow(new IllegalArgumentException("expected exception"));

        sleepMillis(5 * DEFAULT_SLEEP_TIME);

        // when we place an oome file; the processing will stop
        ensureExistingFile(workerProcess.getWorkerHome(), "worker.oome");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFailureType(failureHandler, WORKER_OOME);
            }
        });
    }

    @Test
    public void testRun_shouldContinueAfterErrorResponse() {
        Response failOnceResponse = mock(Response.class);
        when(failOnceResponse.getFirstErrorResponseType()).thenReturn(FAILURE_COORDINATOR_NOT_FOUND).thenReturn(SUCCESS);
        when(failureHandler.handle(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false);

        addWorkerProcess(1);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_ABNORMAL_EXIT);
    }

    @Test
    public void testRun_shouldContinueAfterSendFailure() {
        when(failureHandler.handle(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false)
                .thenReturn(true);

        addWorkerProcess(1);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_ABNORMAL_EXIT);
    }

    @Test
    public void testRun_shouldDetectException_withTestId() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "WorkerProcessFailureMonitorTest", cause);

        System.out.println("ExceptionFile: " + exceptionFile.getAbsolutePath());

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withEmptyTestId() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withNullTestId() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "null", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_shouldRenameFileIfFailureOperationCouldNotBeSent_withSingleErrorResponse() {
        when(failureHandler.handle(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false)
                .thenReturn(true);

        WorkerProcess workerProcess = addRunningWorkerProcess();

        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
        assertThatRenamedExceptionFileExists(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_shouldRenameFileIfFailureOperationCouldNotBeSent_withContinuousErrorResponse() {
        when(failureHandler.handle(
                anyString(), any(FailureType.class), any(WorkerProcess.class), any(String.class), any(String.class)))
                .thenReturn(false);

        WorkerProcess workerProcess = addRunningWorkerProcess();
        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_EXCEPTION);
        assertThatExceptionFileDoesNotExist(exceptionFile);
        assertThatRenamedExceptionFileExists(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withOomeFile() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        ensureExistingFile(workerProcess.getWorkerHome(), "worker.oome");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureType(failureHandler, WORKER_OOME);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withHprofFile() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        ensureExistingFile(workerProcess.getWorkerHome(), "java_pid3140.hprof");

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureType(failureHandler, WORKER_OOME);
    }

    @Test
    public void testRun_shouldDetectInactivity() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFailureTypeAtLeastOnce(failureHandler, WORKER_TIMEOUT);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionDisabled() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureHandler, workerProcessManager, -1,
                DEFAULT_CHECK_INTERVAL);
        workerProcessFailureMonitor.start();

        workerProcessFailureMonitor.startTimeoutDetection();
        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerProcessFailureMonitor.stopTimeoutDetection();
        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureHandler);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionNotStarted() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureHandler);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_afterDetectionIsStopped() {
        WorkerProcess workerProcess = addRunningWorkerProcess();

        workerProcessFailureMonitor.startTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        workerProcessFailureMonitor.stopTimeoutDetection();
        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureHandler);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testRun_shouldDetectWorkerFinished_whenExitValueIsZero() {
        WorkerProcess workerProcess = addWorkerProcess(0);

        do {
            sleepMillis(DEFAULT_SLEEP_TIME);
        } while (!workerProcess.isFinished());

        assertTrue(workerProcess.isFinished());
        assertFailureType(failureHandler, WORKER_NORMAL_EXIT);
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsNonZero() {
        WorkerProcess workerProcess = addWorkerProcess(34);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertFalse(workerProcess.isFinished());
        assertFailureType(failureHandler, WORKER_ABNORMAL_EXIT);
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

    private SimulatorAddress createWorkerAddress() {
        return new SimulatorAddress(WORKER, 1, ++addressIndex, 0);
    }

    private WorkerProcess addRunningWorkerProcess() {
        return addWorkerProcess(null);
    }

    private WorkerProcess addWorkerProcess(Integer exitCode) {
        SimulatorAddress address = createWorkerAddress();

        File sessionHome = new File(workersHome, "sessions");
        File workerHome = new File(sessionHome, "worker" + address.getAddressIndex());
        ensureExistingDirectory(workerHome);

        WorkerProcess workerProcess = new WorkerProcess(address, "WorkerProcessFailureMonitorTest" + address.getAddressIndex(), workerHome);
        Process process = mock(Process.class);

        if (exitCode == null) {
            // this is needed for the failure monitor to believe the process is still running.
            when(process.exitValue()).thenThrow(new IllegalThreadStateException());
        } else {
            when(process.exitValue()).thenReturn(exitCode);
        }

        workerProcess.setProcess(process);
        workerProcessManager.add(address, workerProcess);

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

    private void assertFailureType(WorkerProcessFailureHandler failureHandler, FailureType failureType) {
        assertFailureTypeAtLeastOnce(failureHandler, failureType, times(1));
    }

    private void assertFailureTypeAtLeastOnce(WorkerProcessFailureHandler failureHandler, FailureType failureType) {
        assertFailureTypeAtLeastOnce(failureHandler, failureType, atLeastOnce());
    }

    private void assertFailureTypeAtLeastOnce(WorkerProcessFailureHandler failureHandler, FailureType failureType, VerificationMode mode) {
        verify(failureHandler, mode).handle(anyString(),
                eq(failureType),
                any(WorkerProcess.class),
                any(String.class),
                any(String.class));
        verifyNoMoreInteractions(failureHandler);
    }

    private static void assertThatExceptionFileDoesNotExist(final File firstExceptionFile) {
        // we use assertTrueEventually because the deletion happens on another thread.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse("Exception file should be deleted: " + firstExceptionFile, firstExceptionFile.exists());
            }
        });
    }

    private static void assertThatRenamedExceptionFileExists(File exceptionFile) {
        File expectedFile = new File(exceptionFile.getParentFile(), exceptionFile.getName() + ".sendFailure");
        assertTrue("Exception file should be renamed: " + expectedFile.getName(), expectedFile.exists());
    }
}
