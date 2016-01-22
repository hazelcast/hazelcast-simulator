package com.hazelcast.simulator.agent.workerjvm;

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

public class WorkerJvmManagerTest {

    private SimulatorAddress firstWorkerAddress;
    private SimulatorAddress secondWorkerAddress;

    private WorkerJvm firstWorkerJvm;
    private WorkerJvm secondWorkerJvm;

    private WorkerJvmManager workerJvmManager;

    @Before
    public void setUp() {
        firstWorkerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
        secondWorkerAddress = new SimulatorAddress(WORKER, 1, 2, 0);

        Process process = mock(Process.class);

        firstWorkerJvm = mock(WorkerJvm.class);
        when(firstWorkerJvm.getAddress()).thenReturn(firstWorkerAddress);
        when(firstWorkerJvm.getProcess()).thenReturn(process);

        secondWorkerJvm = mock(WorkerJvm.class);
        when(secondWorkerJvm.getAddress()).thenReturn(secondWorkerAddress);
        when(secondWorkerJvm.getProcess()).thenReturn(process);

        workerJvmManager = new WorkerJvmManager();
        workerJvmManager.add(firstWorkerAddress, firstWorkerJvm);
        workerJvmManager.add(secondWorkerAddress, secondWorkerJvm);
    }

    @Test
    public void testGetWorkerJVMs() {
        Collection<WorkerJvm> workerJVMs = workerJvmManager.getWorkerJVMs();

        assertEquals(2, workerJVMs.size());
        assertTrue(workerJVMs.contains(firstWorkerJvm));
        assertTrue(workerJVMs.contains(secondWorkerJvm));
    }

    @Test
    public void testUpdateLastSeenTimestamp_withResponse() {
        Response response = new Response(1L, COORDINATOR, secondWorkerAddress, ResponseType.SUCCESS);

        workerJvmManager.updateLastSeenTimestamp(response);

        verify(secondWorkerJvm).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromCoordinator() {
        workerJvmManager.updateLastSeenTimestamp(COORDINATOR);

        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromAgent() {
        workerJvmManager.updateLastSeenTimestamp(firstWorkerAddress.getParent());

        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromWorker() {
        workerJvmManager.updateLastSeenTimestamp(firstWorkerAddress);

        verify(firstWorkerJvm).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromUnknownWorker() {
        workerJvmManager.updateLastSeenTimestamp(new SimulatorAddress(WORKER, 2, 1, 0));

        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testUpdateLastSeenTimestamp_withSimulatorAddress_fromTest() {
        workerJvmManager.updateLastSeenTimestamp(secondWorkerAddress.getChild(1));

        verify(secondWorkerJvm).updateLastSeen();
        verifyNoMoreInteractions(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testShutdown() {
        workerJvmManager.shutdown();

        assertEquals(0, workerJvmManager.getWorkerJVMs().size());
        verifyShutdownOfWorkerJvm(firstWorkerJvm);
        verifyShutdownOfWorkerJvm(secondWorkerJvm);
    }

    @Test
    public void testShutdown_withWorkerJVM() {
        workerJvmManager.shutdown(firstWorkerJvm);

        assertEquals(1, workerJvmManager.getWorkerJVMs().size());
        verifyShutdownOfWorkerJvm(firstWorkerJvm);
        verifyNoMoreInteractions(secondWorkerJvm);
    }

    @Test
    public void testShutdown_withWorkerJVM_withException() {
        when(secondWorkerJvm.getProcess()).thenThrow(new RuntimeException("expected exception"));

        workerJvmManager.shutdown(secondWorkerJvm);

        assertEquals(1, workerJvmManager.getWorkerJVMs().size());
        verifyNoMoreInteractions(firstWorkerJvm);
        verifyShutdownOfWorkerJvm(secondWorkerJvm);
    }

    private static void verifyShutdownOfWorkerJvm(WorkerJvm workerJvm) {
        verify(workerJvm).getAddress();
        verify(workerJvm, atLeastOnce()).getProcess();
        verify(workerJvm, atMost(2)).getProcess();
        verifyNoMoreInteractions(workerJvm);
    }
}
