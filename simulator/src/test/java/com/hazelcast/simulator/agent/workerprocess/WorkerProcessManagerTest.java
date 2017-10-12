/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
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
    public void testUpdateLastSeenTimestamp_whenFromResponse_thenUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        Response response = new Response(1L, COORDINATOR, secondWorkerAddress, ResponseType.SUCCESS);
        workerProcessManager.updateLastSeenTimestamp(response);

        assertEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertNotEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
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
    public void testUpdateLastSeenTimestamp_whenSimulatorAddressFromTest_thenUpdate() {
        long firstLastSeen = firstWorkerProcess.getLastSeen();
        long secondLastSeen = secondWorkerProcess.getLastSeen();

        sleepMillis(100);
        workerProcessManager.updateLastSeenTimestamp(secondWorkerAddress.getChild(1));

        assertEquals(firstLastSeen, firstWorkerProcess.getLastSeen());
        assertNotEquals(secondLastSeen, secondWorkerProcess.getLastSeen());
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
