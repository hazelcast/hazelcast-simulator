package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerProcessManagerTest {

    private SimulatorAddress firstWorkerAddress;
    private SimulatorAddress secondWorkerAddress;

    private WorkerProcess firstWorkerProcess;
    private WorkerProcess secondWorkerProcess;

    private WorkerProcessManager workerProcessManager;

    @Before
    public void before() {
        firstWorkerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
        secondWorkerAddress = new SimulatorAddress(WORKER, 1, 2, 0);

        firstWorkerProcess = new WorkerProcess(firstWorkerAddress, firstWorkerAddress.toString(), null);
        firstWorkerProcess.setProcess(mock(Process.class));

        secondWorkerProcess = new WorkerProcess(secondWorkerAddress, secondWorkerAddress.toString(), null);
        secondWorkerProcess.setProcess(mock(Process.class));

        Server server = mock(Server.class);
        workerProcessManager = new WorkerProcessManager(server, SimulatorAddress.fromString("C_A1"), "127.0.0.1");
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
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromCoordinator_thenDoNotUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(COORDINATOR);

        assertEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromAgent_thenDoNotUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(firstWorkerAddress.getParent());

        assertEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromWorker_thenUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(firstWorkerAddress);

        assertNotEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromUnknownWorker_thenDoNotUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(new SimulatorAddress(WORKER, 2, 1, 0));

        assertEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
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
    }

    @Test
    public void testShutdown_withWorkerJVM_withException() {
        Process process = secondWorkerProcess.getProcess();
        doThrow(new RuntimeException("expected exception")).when(process).destroy();

        workerProcessManager.shutdown(secondWorkerProcess);

        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(secondWorkerProcess);
    }

    private static void verifyShutdownOfWorkerJvm(WorkerProcess workerProcess) {
        Process process = workerProcess.getProcess();
        verify(process).destroy();
    }
}
