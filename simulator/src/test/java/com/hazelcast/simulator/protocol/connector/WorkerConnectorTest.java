package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

// this test is without value. It tests the construction of the object, but not that it does anything.
public class WorkerConnectorTest {

    private static final int WORKER_INDEX = 1;
    private static final int AGENT_INDEX = 2;
    private static final int PORT = 11111;

    @Test
    public void testCreateInstance_withFileExceptionLogger() {
        WorkerConnector connector = new WorkerConnector(AGENT_INDEX, WORKER_INDEX, PORT, WorkerType.MEMBER, null, null);
        assertWorkerConnector(connector);
    }

    private void assertWorkerConnector(WorkerConnector connector) {
        SimulatorAddress address = connector.getAddress();
        assertEquals(AddressLevel.WORKER, address.getAddressLevel());
        assertEquals(WORKER_INDEX, address.getWorkerIndex());
        assertEquals(AGENT_INDEX, address.getAgentIndex());

        assertEquals(0, connector.getMessageQueueSize());
        assertEquals(PORT, connector.getPort());
        assertEquals(WorkerOperationProcessor.class, connector.getProcessor().getClass());
    }
}
