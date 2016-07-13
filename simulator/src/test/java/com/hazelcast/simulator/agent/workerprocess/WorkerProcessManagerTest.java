package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerProcessManagerTest {

    private SimulatorAddress firstWorkerAddress;
    private SimulatorAddress secondWorkerAddress;

    private WorkerProcess firstWorkerProcess;
    private WorkerProcess secondWorkerProcess;

    private WorkerProcessManager workerProcessManager;

    @Before
    public void setUp() {
        firstWorkerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
        secondWorkerAddress = new SimulatorAddress(WORKER, 1, 2, 0);

        Process process = mock(Process.class);

        firstWorkerProcess = mock(WorkerProcess.class);
        when(firstWorkerProcess.getAddress()).thenReturn(firstWorkerAddress);
        when(firstWorkerProcess.getProcess()).thenReturn(process);

        secondWorkerProcess = mock(WorkerProcess.class);
        when(secondWorkerProcess.getAddress()).thenReturn(secondWorkerAddress);
        when(secondWorkerProcess.getProcess()).thenReturn(process);

        workerProcessManager = new WorkerProcessManager();
        workerProcessManager.add(firstWorkerAddress, firstWorkerProcess);
        workerProcessManager.add(secondWorkerAddress, secondWorkerProcess);
    }

    @Test
    public void testGetWorkerJVMs() {
        Collection<WorkerProcess> workerProcesses = workerProcessManager.getWorkerProcesses();

        assertEquals(2, workerProcesses.size());
        assertTrue(workerProcesses.contains(firstWorkerProcess));
        assertTrue(workerProcesses.contains(secondWorkerProcess));
    }

    @Test
    public void testUpdateLastSeenTimestamp_withResponse() {
        Response response = new Response(1L, COORDINATOR, secondWorkerAddress, ResponseType.SUCCESS);

        workerProcessManager.updateLastSeenTimestamp(response);

        verify(secondWorkerProcess).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromCoordinator() {
        workerProcessManager.updateLastSeenTimestamp(COORDINATOR);

        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromAgent() {
        workerProcessManager.updateLastSeenTimestamp(firstWorkerAddress.getParent());

        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromWorker() {
        workerProcessManager.updateLastSeenTimestamp(firstWorkerAddress);

        verify(firstWorkerProcess).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromUnknownWorker() {
        workerProcessManager.updateLastSeenTimestamp(new SimulatorAddress(WORKER, 2, 1, 0));

        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromTest() {
        workerProcessManager.updateLastSeenTimestamp(secondWorkerAddress.getChild(1));

        verify(secondWorkerProcess).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testShutdown() {
        workerProcessManager.shutdown();

        assertEquals(0, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(firstWorkerProcess);
        verifyShutdownOfWorkerJvm(secondWorkerProcess);
    }

    @Test
    public void testShutdown_withWorkerJVM() {
        workerProcessManager.shutdown(firstWorkerProcess);

        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(firstWorkerProcess);
        verifyNoMoreInteractions(secondWorkerProcess);
    }

    @Test
    public void testShutdown_withWorkerJVM_withException() {
        when(secondWorkerProcess.getProcess()).thenThrow(new RuntimeException("expected exception"));

        workerProcessManager.shutdown(secondWorkerProcess);

        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
        verifyNoMoreInteractions(firstWorkerProcess);
        verifyShutdownOfWorkerJvm(secondWorkerProcess);
    }

    private static void verifyShutdownOfWorkerJvm(WorkerProcess workerProcess) {
        verify(workerProcess).getAddress();
        verify(workerProcess, atLeastOnce()).getProcess();
        verify(workerProcess, atMost(2)).getProcess();
        verifyNoMoreInteractions(workerProcess);
    }
}
