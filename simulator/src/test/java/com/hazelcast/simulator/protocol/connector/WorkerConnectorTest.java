package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.connector.WorkerConnector.createInstance;
import static org.junit.Assert.assertEquals;

public class WorkerConnectorTest {

    private static final int WORKER_INDEX = 1;
    private static final int AGENT_INDEX = 2;
    private static final int PORT = 11111;

    @Test
    public void testCreateInstance_withFileExceptionLogger_implicit() {
        WorkerConnector connector = createInstance(AGENT_INDEX, WORKER_INDEX, PORT, WorkerType.MEMBER, null, null);
        assertWorkerConnector(connector);
    }

    @Test
    public void testCreateInstance_withRemoteExceptionLogger_explicit() {
        WorkerConnector connector = createInstance(AGENT_INDEX, WORKER_INDEX, PORT, WorkerType.MEMBER, null, null, false);
        assertWorkerConnector(connector);
    }

    @Test
    public void testCreateInstance_withRemoteExceptionLogger() {
        WorkerConnector connector = createInstance(AGENT_INDEX, WORKER_INDEX, PORT, WorkerType.MEMBER, null, null, true);
        assertWorkerConnector(connector);
    }

    private void assertWorkerConnector(WorkerConnector connector) {
        SimulatorAddress address = connector.getAddress();
        assertEquals(AddressLevel.WORKER, address.getAddressLevel());
        assertEquals(WORKER_INDEX, address.getWorkerIndex());
        assertEquals(AGENT_INDEX, address.getAgentIndex());

        assertEquals(PORT, connector.getPort());
        assertEquals(WorkerOperationProcessor.class, connector.getProcessor().getClass());
    }
}
