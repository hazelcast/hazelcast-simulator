package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerJvmFailureMonitorTest {

    private AgentConnector agentConnector;

    private File firstWorkerHome;
    private File secondWorkerHome;
    private File thirdWorkerHome;

    private WorkerJvmFailureMonitor workerJvmFailureMonitor;

    @Before
    public void setUp() {
        Response response = mock(Response.class);

        agentConnector = mock(AgentConnector.class);
        when(agentConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);

        Agent agent = mock(Agent.class);
        when(agent.getAgentConnector()).thenReturn(agentConnector);

        WorkerJvmManager workerJvmManager = new WorkerJvmManager();

        firstWorkerHome = addWorkerJvm(workerJvmManager, new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), true);
        secondWorkerHome = addWorkerJvm(workerJvmManager, new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0), true);
        thirdWorkerHome = addWorkerJvm(workerJvmManager, new SimulatorAddress(AddressLevel.WORKER, 1, 3, 0), true);
        addWorkerJvm(workerJvmManager, new SimulatorAddress(AddressLevel.WORKER, 1, 4, 0), false);

        workerJvmFailureMonitor = new WorkerJvmFailureMonitor(agent, workerJvmManager, 100);
    }

    @After
    public void tearDown() {
        workerJvmFailureMonitor.shutdown();

        deleteQuiet(firstWorkerHome);
        deleteQuiet(secondWorkerHome);
        deleteQuiet(thirdWorkerHome);
    }

    @Test
    public void testRun_shouldSendNoFailures() {
        sleepMillis(300);

        verifyNoMoreInteractions(agentConnector);
    }

    @Test
    public void testRun_shouldDetectException() {
        sleepMillis(300);

        String cause = throwableToString(new RuntimeException());
        File firstExceptionFile = createExceptionFile(firstWorkerHome, "WorkerJvmFailureMonitorTest", cause);
        File secondExceptionFile = createExceptionFile(secondWorkerHome, "", cause);
        File thirdExceptionFile = createExceptionFile(thirdWorkerHome, "null", cause);

        sleepMillis(200);

        verify(agentConnector, times(3)).write(eq(COORDINATOR), any(FailureOperation.class));
        verifyNoMoreInteractions(agentConnector);
        assertThatExceptionFileDoesNotExist(firstExceptionFile);
        assertThatExceptionFileDoesNotExist(secondExceptionFile);
        assertThatExceptionFileDoesNotExist(thirdExceptionFile);
    }

    @Test
    public void testRun_shouldDetectOomeFailure() {

    }

    private static File addWorkerJvm(WorkerJvmManager workerJvmManager, SimulatorAddress address, boolean createWorkerHome) {
        Process process = mock(Process.class);
        when(process.exitValue()).thenThrow(new IllegalThreadStateException("process is still running"));

        int addressIndex = address.getAddressIndex();
        File workerHome = new File("worker" + address.getAddressIndex());
        WorkerJvm workerJvm = new WorkerJvm(address, "WorkerJvmFailureMonitorTest" + addressIndex, workerHome);
        workerJvm.setProcess(process);

        workerJvmManager.add(address, workerJvm);

        if (createWorkerHome) {
            ensureExistingDirectory(workerHome);
        }

        return workerHome;
    }

    private static File createExceptionFile(File workerHome, String testId, String cause) {
        String targetFileName = "1.exception";

        File tmpFile = new File(workerHome, targetFileName + "tmp");
        File exceptionFile = new File(workerHome, targetFileName);

        ensureExistingFile(tmpFile);
        appendText(testId + NEW_LINE + cause, tmpFile);
        rename(tmpFile, exceptionFile);

        return exceptionFile;
    }

    private static void assertThatExceptionFileDoesNotExist(File firstExceptionFile) {
        if (firstExceptionFile.exists()) {
            fail("Exception file should be deleted: " + firstExceptionFile);
        }
    }
}
