package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.coordinatorAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerProcessManagerTest {

    private SimulatorAddress workerAddress1;
    private SimulatorAddress workerAddress2;

    private WorkerProcess workerProcess1;
    private WorkerProcess workerProcess2;

    private WorkerProcessManager workerProcessManager;

    @Before
    public void before() {
        workerAddress1 = workerAddress(1, 1);
        workerAddress2 = workerAddress(1, 2);

        workerProcess1 = new WorkerProcess(workerAddress1, workerAddress1.toString(), null);
        workerProcess1.setProcess(mock(Process.class));

        workerProcess2 = new WorkerProcess(workerAddress2, workerAddress2.toString(), null);
        workerProcess2.setProcess(mock(Process.class));

        Server server = mock(Server.class);
        workerProcessManager = new WorkerProcessManager(server, workerAddress1.getParent(), "127.0.0.1");
        workerProcessManager.add(workerAddress1, workerProcess1);
        workerProcessManager.add(workerAddress2, workerProcess2);
    }

    @Test
    public void testGetWorkerJVMs() {
        Collection<WorkerProcess> workerProcesses = workerProcessManager.getWorkerProcesses();

        assertEquals(2, workerProcesses.size());
        assertTrue(workerProcesses.contains(workerProcess1));
        assertTrue(workerProcesses.contains(workerProcess2));
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromCoordinator_thenDoNotUpdate() {
        long firstLastSeen = workerProcess1.getLastSeen();
        long secondLastSeen = workerProcess2.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(coordinatorAddress());

        assertEquals(firstLastSeen, workerProcess1.getLastSeen());
        assertEquals(secondLastSeen, workerProcess2.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromAgent_thenDoNotUpdate() {
        long firstLastSeen = workerProcess1.getLastSeen();
        long secondLastSeen = workerProcess2.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(workerAddress1.getParent());

        assertEquals(firstLastSeen, workerProcess1.getLastSeen());
        assertEquals(secondLastSeen, workerProcess2.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromWorker_thenUpdate() {
        long firstLastSeen = workerProcess1.getLastSeen();
        long secondLastSeen = workerProcess2.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(workerAddress1);

        assertNotEquals(firstLastSeen, workerProcess1.getLastSeen());
        assertEquals(secondLastSeen, workerProcess2.getLastSeen());
    }

    @Test
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromUnknownWorker_thenDoNotUpdate() {
        long firstLastSeen = workerProcess1.getLastSeen();
        long secondLastSeen = workerProcess2.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(workerAddress(2, 1));

        assertEquals(firstLastSeen, workerProcess1.getLastSeen());
        assertEquals(secondLastSeen, workerProcess2.getLastSeen());
    }

    @Test
    public void testShutdown() {
        workerProcessManager.shutdown();

        assertEquals(0, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(workerProcess1);
        verifyShutdownOfWorkerJvm(workerProcess2);
    }

    @Test
    public void testShutdown_withWorkerJVM() {
        workerProcessManager.shutdown(workerProcess1);

        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(workerProcess1);
    }

    @Test
    public void testShutdown_withWorkerJVM_withException() {
        Process process = workerProcess2.getProcess();
        doThrow(new RuntimeException("expected exception")).when(process).destroy();

        workerProcessManager.shutdown(workerProcess2);

        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
        verifyShutdownOfWorkerJvm(workerProcess2);
    }

    private static void verifyShutdownOfWorkerJvm(WorkerProcess workerProcess) {
        Process process = workerProcess.getProcess();
        verify(process).destroy();
    }
}
